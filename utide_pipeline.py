#!/usr/bin/env python3
"""
utide_pipeline.py  —  UTide 調和分析 / 預報正式流程（取代 tide_pipeline.py 的 Fortran 四步驟）

參數、寫入的表與 flag 規則都與 tide_pipeline.py 相同，DB 讀寫共用 tide_db.py：
  分析（flag a/h）：tide6 整點水位 → UTide solve → tidesub（寫入前與既有常數比對，不同就停）
  預報：常數 → 逐分鐘重建 → tide6ha（flag a/h）或 tide1ha（flag c）
                         → 高低潮（定義同 TIDE4R2b）→ 平潮標記 → tideh（flag a/h）
  只預報（--predict-only）與 flag c：從 tidesub 讀常數（flag c 為外部匯入的 Foreman 常數）

預報一律使用寫入 tidesub 精度（A 0.0001 m、G 0.01°）的常數與全部分潮，
所以「分析後直接預報」與日後「從 tidesub 讀常數預報」結果相同（Fortran 亦同：TIDE4R2b 讀 .har）。
UTide 沒有 Foreman AL/GL 的對應量，tidesub.APLITUDE/PHASEL 寫 NULL（預報不使用）。

執行方式：
  python utide_pipeline.py --stid 1516 --start 2024-01-01 --end 2025-12-31 --flag h \\
                           --predbeg 2026-01-01 --predend 2026-12-31
  python utide_pipeline.py --stid 1516 --start 2024-01-01 --end 2025-12-31 --flag h \\
                           --predbeg 2027-01-01 --predend 2027-12-31 --predict-only
  python utide_pipeline.py --stid 1516 --flag c --predbeg 2026-01-01 --predend 2026-12-31
  python utide_pipeline.py --stid 1516 --list-tidesub
  # 平行測試（只限 local）：寫入 tideh_u / tide6ha_u / tide1ha_u / tidesub_u
  python utide_pipeline.py ... --table-suffix _u

依賴：mysql-connector-python、utide（numpy、scipy）
"""

import argparse
import logging
import sys
import warnings
from datetime import date, datetime, timedelta

import numpy as np

# utide 匯入分潮表時對 NaN 轉整數發出的 RuntimeWarning（無害）
warnings.filterwarnings("ignore", category=RuntimeWarning, module=r"utide\.")
import utide  # noqa: E402

import tide_utide as tu
from tide_db import (
    ENV, select_env, get_conn,
    fetch_station_meta, fetch_water_levels, fetch_tidesub_constants, parse_lat_lon_from_meta,
    check_tidesub, list_tidesub, write_outputs, apply_slack_tide_marking,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(description="UTide 調和分析 / 預報流程（寫入規則同 tide_pipeline.py）")
    p.add_argument("--stid", required=True, help="站碼，例如 1516")
    p.add_argument("--start", default=None,
                   help="分析起始日 YYYY-MM-DD（→ tidesub.DATEBEG）；flag a/h 必填，"
                        "--predict-only 時用來指定沿用哪次分析的常數；flag c 不使用")
    p.add_argument("--end", default=None, help="分析結束日 YYYY-MM-DD（→ tidesub.DATEEND）；用法同 --start")
    p.add_argument("--flag", choices=["a", "h", "c"], default="h",
                   help="a/h：分析寫 tidesub，預報寫 tide6ha + tideh；c：讀 tidesub ah='c' 的常數預報，只寫 tide1ha")
    p.add_argument("--predbeg", default=None,
                   help="預報起始日 YYYY-MM-DD（需與 --predend 同時提供）；省略時只分析，tidesub.predbeg 寫 NULL")
    p.add_argument("--predend", default=None, help="預報結束日 YYYY-MM-DD；用法同 --predbeg")
    p.add_argument("--predict-only", action="store_true",
                   help="flag a/h：不重新分析，改讀 tidesub (STID, ah, DATEBEG=--start, DATEEND=--end) 的常數預報，"
                        "並以本次預報期間另存一份 tidesub 紀錄")
    p.add_argument("--supersede", action="store_true",
                   help="分析模式：tidesub 同一分析期間已有不同的常數時，以本次結果取代；未加時遇到不同會停止、不寫入")
    p.add_argument("--list-tidesub", action="store_true",
                   help="只列出 --stid 在 tidesub 的全部分組，不執行流程")
    p.add_argument("--skip-db-write", action="store_true", help="計算但不寫入 DB")
    p.add_argument("--stid-new", default=None, help="STID_NEW（新站碼，選填；預設取站點表）")
    p.add_argument("--env", choices=["local", "mirror", "prod"], default="local", help="執行環境（預設 local）")
    p.add_argument("--table-suffix", default="",
                   help="寫入表名加後綴（平行測試用，例如 _u → tideh_u；只允許 --env local）。"
                        "flag c 的常數仍讀正式 tidesub")
    return p.parse_args()


def _parse_date(value: str | None, name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        sys.exit(f"{name} 日期格式錯誤：{value}")


def _day_start(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time())


def main():
    args = parse_args()
    try:
        select_env(args.env, args.table_suffix)
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)
    log.info(f"執行環境：{args.env}  DB={ENV['db']['host']}/{ENV['db']['database']}"
             + (f"  寫入表後綴={args.table_suffix}" if args.table_suffix else ""))

    if args.list_tidesub:
        conn = get_conn()
        try:
            print(list_tidesub(conn, args.stid))
        finally:
            conn.close()
        return

    # ── 參數檢查（規則同 tide_pipeline.py）──
    flag = args.flag
    start_date, end_date = _parse_date(args.start, "--start"), _parse_date(args.end, "--end")
    pred_start, pred_end = _parse_date(args.predbeg, "--predbeg"), _parse_date(args.predend, "--predend")
    if (start_date is None) != (end_date is None):
        sys.exit("--start 與 --end 需同時提供或同時省略")
    if (pred_start is None) != (pred_end is None):
        sys.exit("--predbeg 與 --predend 需同時提供或同時省略")
    if start_date is not None and start_date > end_date:
        sys.exit("起始日期不得晚於結束日期")
    do_predict = pred_start is not None
    if do_predict and pred_start > pred_end:
        sys.exit("預報起始日期不得晚於結束日期")
    from_db = flag == "c" or args.predict_only
    if from_db and not do_predict:
        sys.exit("flag c 或 --predict-only 只做預報，需提供 --predbeg/--predend")
    if flag != "c" and start_date is None:
        sys.exit("flag a/h 需提供 --start/--end（分析期間，或 --predict-only 時沿用哪次分析的常數）")
    if args.supersede and from_db:
        sys.exit("--supersede 只用於分析模式（重新分析後取代既有常數）")

    mode = "只預報（tidesub 常數）" if from_db else ("分析+預報" if do_predict else "只分析")
    log.info(f"=== UTide 流程開始 ===  utide {utide.__version__}  站碼={args.stid}  flag={flag}  模式={mode}"
             f"  分析={start_date}~{end_date}"
             f"  pred={f'{pred_start}~{pred_end}' if do_predict else '（不預報）'}")

    conn = get_conn()
    try:
        meta = fetch_station_meta(conn, args.stid)
        stid_new = args.stid_new or meta.get("stid_new")
        lat_deg, lat_min, _, _ = parse_lat_lon_from_meta(meta)
        lat = lat_deg + lat_min / 60
        log.info(f"站點資訊：{args.stid}（{meta.get('stnac', '')}）  緯度={lat_deg}°{lat_min}'")

        # ── 分潮常數：讀 tidesub，或由觀測資料分析 ──
        supersede: list[tuple] = []
        if from_db:
            constants, start_date, end_date = fetch_tidesub_constants(
                conn, args.stid, flag, start_date, end_date)
        else:
            times, heights = tu.hourly_from_daily(
                fetch_water_levels(conn, args.stid, start_date, end_date))
            log.info(f"UTide 分析：{len(times)} 筆整點觀測")
            constants = tu.constants_to_records(tu.solve_constants(times, heights, lat))
            log.info(f"[分析完成] {len(constants)} 個分潮（含 Z0）")

            report, conflicts = check_tidesub(conn, args.stid, flag, start_date, end_date, constants)
            log.info("\n" + report)
            if conflicts:
                if args.skip_db_write:
                    log.warning("tidesub 既有常數與本次不同（--skip-db-write：僅報告，"
                                "即使加了 --supersede 也不會刪除或寫入）")
                elif args.supersede:
                    supersede = conflicts
                    log.warning(f"--supersede：將以本次結果取代以上 {len(conflicts)} 組不同的舊常數")
                else:
                    log.error("tidesub 既有常數與本次不同，未寫入任何資料。"
                              "確認要以本次結果取代以上舊組，請加 --supersede 重跑。")
                    sys.exit(2)

        # ── 預報：一律用 tidesub 精度的常數（與從 DB 讀回時相同）──
        minute_records: list[dict] = []
        extreme_records: list[dict] = []
        if do_predict:
            midpoint = _day_start(start_date) + (_day_start(end_date) - _day_start(start_date)) / 2
            coef, skipped = tu.coef_from_constants(constants, lat, midpoint)
            if skipped:
                log.warning(f"UTide 分潮表沒有以下分潮，預報時略過：{', '.join(skipped)}")

            # 前後各多算一天，讓期間首末的高低潮也找得到（同 trans2 前後延一天）
            p0 = _day_start(pred_start)
            p1 = _day_start(pred_end + timedelta(days=1))
            t_ext, h_ext = tu.reconstruct_minutes(coef, p0 - timedelta(days=1), p1 + timedelta(days=1))
            sel = (t_ext >= np.datetime64(p0, "m")) & (t_ext < np.datetime64(p1, "m"))
            minute_records = [
                {"dt": t, "height_mm": int(round(h))}
                for t, h in zip(t_ext[sel].astype("datetime64[s]").astype(datetime).tolist(),
                                h_ext[sel].tolist())
            ]
            log.info(f"[預報完成] {len(minute_records)} 筆逐分鐘預報（{pred_start}~{pred_end}）")

            if flag != "c":
                events = tu.find_extremes(t_ext, h_ext)
                records = tu.extremes_to_records(events, args.stid, stid_new, flag)
                # 先標平潮再篩選，讓預報期間首末筆也能與期間外的相鄰事件比較
                records = apply_slack_tide_marking(records)
                extreme_records = [r for r in records
                                   if pred_start <= r["DATATIME"].date() <= pred_end]
                log.info(f"[高低潮] {len(extreme_records)} 筆")
        else:
            log.info("未指定 --predbeg/--predend：不預報")

        # ── 寫入（規則見 tide_db.write_outputs）──
        if args.skip_db_write:
            log.info("[skip-db-write] 跳過寫入 DB")
        else:
            write_outputs(
                conn,
                stid=args.stid, stid_new=stid_new, flag=flag,
                pred_start=pred_start, pred_end=pred_end,
                minute_records=minute_records, extreme_records=extreme_records,
                constants=constants, datebeg=start_date, dateend=end_date,
                supersede=supersede,
            )
        log.info("=== UTide 流程完成 ===")

    except Exception as e:
        log.exception(f"流程失敗：{e}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
