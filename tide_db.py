#!/usr/bin/env python3
"""
tide_db.py  —  潮汐流程共用的資料庫層（Fortran 的 tide_pipeline.py 與 UTide 流程共用）

  設定：.env 讀取、ENVIRONMENTS、select_env()、get_conn()
  讀取：fetch_station_meta、fetch_water_levels、fetch_tidesub_constants、parse_lat_lon_from_meta
  檢查：check_tidesub（同一分析期間既有常數與本次比對）、list_tidesub（某站全部分組總覽）
  寫入：write_outputs（依 flag 分流，交易內完成）、write_tide6ha、write_tide1ha、
        replace_tideh、write_tidesub、apply_slack_tide_marking

寫入策略（重跑、改用其他方法重算時都不會留下舊資料）：
  tide6ha / tide1ha  upsert，並明寫 LASTUPDATETIME：鍵是固定時間格點，預報期間內每個鍵都會被覆寫。
                     值未變時 MySQL 不觸發 ON UPDATE CURRENT_TIMESTAMP，所以需明寫。
  tideh              先刪預報期間再插入：PK 含分鐘，高低潮時刻一變，upsert 會殘留舊事件。
  tidesub            先刪同組 (STID, DATEBEG, DATEEND, ah, predbeg, predend) 再插入；
                     不依賴 PK（.71 沒有 PK），未預報時 predbeg/predend 為 NULL（同舊系統）。
flag a/h 的 tide6ha + tideh + tidesub 在同一交易內 commit；flag c 的 tide1ha 每月 commit 一次。
SQL 需相容 MySQL 5.7（南區 .26）：不用 CTE、視窗函數、INSERT ... AS alias。
"""

import logging
import math
import os
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path

import mysql.connector

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  設定：.env 與環境
# ─────────────────────────────────────────────

def _load_dotenv(path: Path):
    """
    讀取 .env（KEY=VALUE，每行一筆，# 開頭為註解，值可用引號包住）。
    已存在的環境變數優先，不會被 .env 覆寫。
    不依賴 python-dotenv，避免部署環境需另外安裝套件。
    """
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key, value = key.strip(), value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        os.environ.setdefault(key, value)


# 專案根目錄（本模組所在目錄）；所有預設路徑都相對於此，不寫死絕對路徑
BASE_DIR = Path(__file__).resolve().parent

_load_dotenv(BASE_DIR / ".env")


def _db_from_env(prefix: str) -> dict:
    """由環境變數 {prefix}_DB_HOST / _USER / _PASSWORD / _NAME 組出連線設定。"""
    return dict(
        host=os.environ.get(f"{prefix}_DB_HOST"),
        port=int(os.environ.get(f"{prefix}_DB_PORT", "3306")),
        user=os.environ.get(f"{prefix}_DB_USER"),
        password=os.environ.get(f"{prefix}_DB_PASSWORD", ""),
        database=os.environ.get(f"{prefix}_DB_NAME"),
        # 3-byte UTF-8（utf8_general_ci）的編號 33。不寫名稱是因為同一個編碼在
        # MySQL 8 叫 utf8mb3、在 MySQL 5.x / MariaDB 叫 utf8，mysql-connector 依伺服器版本
        # 只認其中一個名稱（南區 .26 用 'utf8mb3' 會報 "Character set 'utf8mb3' unsupported"）；
        # 用編號則兩種伺服器都適用，連線後送出各自正確的 SET NAMES。
        charset=33,
    )


_TABLES = dict(
    water_table="tide6",
    station_table="st",
    output_table="tideh",
    output_6ha_table="tide6ha",
    output_1ha_table="tide1ha",
    output_sub_table="tidesub",
    # flag c 的常數由外部匯入，永遠從正式 tidesub 讀（不加 --table-suffix）
    const_c_table="tidesub",
)

# 連線帳密由 .env（或系統環境變數）提供，見 .env.example；三個環境表名相同
ENVIRONMENTS = {
    "local":  dict(db=_db_from_env("LOCAL"),  **_TABLES),
    "mirror": dict(db=_db_from_env("MIRROR"), **_TABLES),
    "prod":   dict(db=_db_from_env("PROD"),   **_TABLES),
}

# 寫入表加 --table-suffix 時會改名的鍵（平行測試用，例如 tideh_u）
_OUTPUT_KEYS = ("output_table", "output_6ha_table", "output_1ha_table", "output_sub_table")

# 目前環境；由 select_env() 就地填入（其他模組 import 後持有同一個 dict）
ENV: dict = {}


def select_env(name: str, table_suffix: str = "") -> dict:
    """
    依 --env 填入 ENV，並檢查連線設定是否齊全（缺少時丟 ValueError）。
    table_suffix 非空時，寫入表名加上後綴（只允許 local，避免在正式環境建出影子表）。
    """
    if table_suffix and name != "local":
        raise ValueError("--table-suffix 只允許在 --env local 使用")
    cfg = dict(ENVIRONMENTS[name])
    for k in _OUTPUT_KEYS:
        cfg[k] = cfg[k] + table_suffix
    prefix = name.upper()
    missing = [f"{prefix}_DB_{k}" for k, v in
               (("HOST", cfg["db"]["host"]), ("USER", cfg["db"]["user"]),
                ("NAME", cfg["db"]["database"])) if not v]
    if missing:
        raise ValueError(f"缺少資料庫設定：{', '.join(missing)}（請於 .env 填入，見 .env.example）")
    ENV.clear()
    ENV.update(cfg)
    return ENV


def get_conn():
    """依目前 ENV 取得連線（autocommit 關閉，寫入由各函式明確 commit）。"""
    return mysql.connector.connect(**ENV["db"])


# ─────────────────────────────────────────────
#  讀取
# ─────────────────────────────────────────────

def fetch_station_meta(conn, stid: str) -> dict:
    """從站點表（st）取得站點元資料（整列）。"""
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT * FROM {ENV['station_table']} WHERE stid = %s", (stid,))
    row = cur.fetchone()
    cur.close()
    if row is None:
        raise ValueError(f"{ENV['station_table']} 中找不到 stid={stid}")
    return row


def parse_lat_lon_from_meta(meta: dict) -> tuple[int, int, int, int]:
    """站點表的 lat/lon（十進位度）分解成 (緯度度, 緯度分, 經度度, 經度分)。"""
    if "lat" in meta and meta["lat"] is not None:
        lat_f = float(meta["lat"])
        lon_f = float(meta["lon"])
        lat_deg = int(abs(lat_f))
        lat_min = round((abs(lat_f) - lat_deg) * 60)
        lon_deg = int(abs(lon_f))
        lon_min = round((abs(lon_f) - lon_deg) * 60)
        return lat_deg, lat_min, lon_deg, lon_min
    raise ValueError(f"無法從站點表解析座標，請確認欄位名稱：{list(meta.keys())}")


def fetch_water_levels(conn, stid: str, start_date: date, end_date: date) -> list[dict]:
    """
    從水位表（tide6）取得 qc='Q' 的整點水位（MIN0 = 該小時 00 分，毫米）。

    回傳逐日 list，每個 dict：year, month, day, hours（list[int] 長度 24，00~23 時，缺值 9999）。
    缺日也會補一列全 9999，日數 = end_date - start_date + 1。
    """
    sql = f"""
        SELECT YEAR, MONTH, DAY, HOUR,
               CAST(MIN0 AS SIGNED) AS wl
        FROM {ENV['water_table']}
        WHERE STID = %s
          AND DATATIME >= %s
          AND DATATIME < %s
          AND qc = 'Q'
        ORDER BY YEAR, MONTH, DAY, HOUR
    """
    dt_start = datetime.combine(start_date, datetime.min.time())
    dt_end   = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, (stid, dt_start, dt_end))
    rows = cur.fetchall()
    cur.close()

    if not rows:
        raise ValueError(
            f"{ENV['water_table']} 中查無 stid={stid} 的資料 ({start_date} ~ {end_date})"
        )

    day_map: dict[tuple, dict[int, int]] = {}
    for r in rows:
        key = (int(r["YEAR"]), int(r["MONTH"]), int(r["DAY"]))
        wl  = int(r["wl"]) if r["wl"] is not None else 9999
        day_map.setdefault(key, {})[int(r["HOUR"])] = wl

    result = []
    cur_date = start_date
    while cur_date <= end_date:
        hmap = day_map.get((cur_date.year, cur_date.month, cur_date.day), {})
        result.append({
            "year":  cur_date.year,
            "month": cur_date.month,
            "day":   cur_date.day,
            "hours": [hmap.get(h, 9999) for h in range(24)],
        })
        cur_date += timedelta(days=1)

    log.info(f"已讀取 {len(result)} 天水位資料（{start_date} ~ {end_date}）")
    return result


def fetch_tidesub_constants(conn, stid: str, ah: str,
                            datebeg: date | None = None,
                            dateend: date | None = None) -> tuple[list[dict], date, date]:
    """
    讀 tidesub 中 (STID, ah[, DATEBEG, DATEEND]) 的分潮常數，回傳 (records, DATEBEG, DATEEND)。

    同一次分析每做一次預報就另存一份（predbeg/predend 不同），常數相同；取最近更新的一份。
    未指定 DATEBEG/DATEEND 時（flag c），該站只能有一組分析期間，否則報錯請使用者指定。
    flag c 一律讀正式 tidesub（ENV['const_c_table']），a/h 讀本次寫入的表。
    APLITUDE/PHASEL 可能為 NULL（預報不使用），回傳 None。
    """
    table = ENV["const_c_table"] if ah == "c" else ENV["output_sub_table"]
    sql = f"""
        SELECT DATEBEG, DATEEND, predbeg, predend, LASTUPDATETIME,
               TRIM(NAME) AS NAME, FREQUENCY, AMPLITUDE, PHASE, APLITUDE, PHASEL
        FROM {table}
        WHERE STID = %s AND ah = %s
    """
    params: list = [stid, ah]
    if datebeg is not None:
        sql += " AND DATEBEG = %s AND DATEEND = %s"
        params += [datebeg, dateend]
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()

    where = f"STID={stid}, ah='{ah}'" + (f", DATEBEG={datebeg}, DATEEND={dateend}" if datebeg else "")
    if not rows:
        raise ValueError(f"{table} 中查無分潮常數（{where}）")

    periods = {(r["DATEBEG"], r["DATEEND"]) for r in rows}
    if len(periods) > 1:
        listed = "、".join(f"{b}~{e}" for b, e in sorted(periods))
        raise ValueError(f"{table} 中有多組分析期間（{where}）：{listed}，請以 --start/--end 指定")

    groups = _group_by_pred(rows)
    latest = max(groups.values(),
                 key=lambda g: max((r["LASTUPDATETIME"] or datetime.min) for r in g))

    def num(v):
        return float(v) if v is not None else None

    records = [
        {"NAME": r["NAME"], "FREQUENCY": num(r["FREQUENCY"]),
         "AMPLITUDE": num(r["AMPLITUDE"]), "PHASE": num(r["PHASE"]),
         "APLITUDE": num(r["APLITUDE"]), "PHASEL": num(r["PHASEL"])}
        for r in sorted(latest, key=lambda r: r["FREQUENCY"])
    ]
    (db_beg, db_end), = periods
    log.info(f"[{table}] 讀取 {len(records)} 個分潮常數（{where}，分析期間 {db_beg}~{db_end}）")
    return records, db_beg, db_end


def _group_by_pred(rows: list[dict]) -> dict[tuple, list[dict]]:
    """依 (predbeg, predend) 分組；NULL（未預報）以 None 表示。"""
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        groups.setdefault((r["predbeg"], r["predend"]), []).append(r)
    return groups


# ─────────────────────────────────────────────
#  tidesub 檢查與總覽
# ─────────────────────────────────────────────

# 比對容許誤差 = DB 欄位精度一個單位（A DECIMAL(6,4) 公尺、G DECIMAL(6,2) 度），
# 吸收不同機器編譯的執行檔在最後一位的捨入差異
_TOL_A = 0.0001 + 1e-9
_TOL_G = 0.01 + 1e-9
# 報告中只對振幅 ≥ 5 mm 的分潮列相位差（小振幅相位本來就不穩定）
_REPORT_G_MIN_A = 0.005


def _pad(text: str, width: int) -> str:
    """依顯示寬度補空白（全形字算 2 格），讓含中文的欄位在終端機對齊。"""
    w = sum(2 if unicodedata.east_asian_width(ch) in "WF" else 1 for ch in text)
    return text + " " * max(0, width - w)


def _pred_label(pb, pe) -> str:
    return "（未預報）" if pb is None else f"{pb}~{pe}"


def _wrap_deg(d: float) -> float:
    return (d + 180.0) % 360.0 - 180.0


def _compare_constants(new: dict[str, dict], old: dict[str, dict]) -> dict:
    """
    比對兩組常數（NAME → {AMPLITUDE, PHASE}，單位 m / 度）。
    回傳 kind：'same' 相同、'z0' 只有 Z0 不同（疑似基準調整）、'diff' 其他差異，及細節。
    """
    missing = sorted(set(old) - set(new), key=lambda n: old[n]["FREQUENCY"] or 0)
    added = sorted(set(new) - set(old), key=lambda n: new[n]["FREQUENCY"] or 0)
    z0_new = new.get("Z0", {}).get("AMPLITUDE")
    z0_old = old.get("Z0", {}).get("AMPLITUDE")
    dz0 = (z0_new - z0_old) if (z0_new is not None and z0_old is not None) else None

    # 每個分潮的「向量差」|A1·e^(iG1) − A2·e^(iG2)|：該分潮對預報水位造成的最大差異；
    # 加總即為兩組常數預報差異的上限（不含 Z0）
    changed = []          # (NAME, dA, dG, 振幅, 向量差)，單位 m / 度
    max_da = max_dg = sum_dv = 0.0
    for n in set(new) & set(old):
        if n == "Z0":
            continue
        a_new, a_old = new[n]["AMPLITUDE"], old[n]["AMPLITUDE"]
        da = a_new - a_old
        dg = _wrap_deg(new[n]["PHASE"] - old[n]["PHASE"])
        dv = math.sqrt(max(0.0, a_new ** 2 + a_old ** 2
                           - 2 * a_new * a_old * math.cos(math.radians(dg))))
        amp = max(a_new, a_old)
        max_da = max(max_da, abs(da))
        if amp >= _REPORT_G_MIN_A:
            max_dg = max(max_dg, abs(dg))
        if abs(da) > _TOL_A or abs(dg) > _TOL_G:
            changed.append((n, da, dg, amp, dv))
            sum_dv += dv

    z0_changed = dz0 is not None and abs(dz0) > _TOL_A
    if not missing and not added and not changed:
        kind = "z0" if z0_changed else "same"
    else:
        kind = "diff"
    changed.sort(key=lambda c: -c[4])
    return dict(kind=kind, dz0=dz0, missing=missing, added=added,
                changed=changed, max_da=max_da, max_dg=max_dg, sum_dv=sum_dv,
                n_common=len(set(new) & set(old) - {"Z0"}), n_old=len(old))


def check_tidesub(conn, stid: str, ah: str, datebeg: date, dateend: date,
                  constants: list[dict]) -> tuple[str, list[tuple]]:
    """
    分析模式寫入 tidesub 前，比對同一 (STID, ah, DATEBEG, DATEEND) 底下既有各組常數。

    回傳 (報告文字, 與本次不同的組鍵 [(predbeg, predend), ...])。
    有不同的組時，呼叫端應停止（除非 --supersede，再把這些組鍵交給 write_outputs 刪除）。
    """
    table = ENV["output_sub_table"]
    cur = conn.cursor(dictionary=True)
    cur.execute(
        f"""SELECT predbeg, predend, LASTUPDATETIME, TRIM(NAME) AS NAME,
                   FREQUENCY, AMPLITUDE, PHASE
            FROM {table}
            WHERE STID = %s AND ah = %s AND DATEBEG = %s AND DATEEND = %s""",
        (stid, ah, datebeg, dateend),
    )
    rows = cur.fetchall()
    cur.close()

    new = {r["NAME"]: r for r in constants}
    lines = [f"[tidesub 檢查] STID={stid} ah={ah} 分析期間 {datebeg}~{dateend}（{table}）",
             "本次計算：" + _summary(new)]
    if not rows:
        lines.append("同一分析期間沒有既有常數。")
        return "\n".join(lines), []

    groups = _group_by_pred(rows)
    keys = sorted(groups, key=lambda k: (k[0] is not None, k[0] or date.min, k[1] or date.min))
    lines.append(f"\n同一分析期間既有 {len(keys)} 組：")
    lines.append(f" {'#':>2}  {_pad('預報期間', 23)} {_pad('分潮數', 6)} {'Z0(mm)':>8}  {_pad('最後更新', 16)}  與本次比較")

    results = []
    for i, k in enumerate(keys, 1):
        g = groups[k]
        old = {}
        for r in g:
            old[r["NAME"]] = {"FREQUENCY": float(r["FREQUENCY"] or 0),
                              "AMPLITUDE": float(r["AMPLITUDE"] or 0),
                              "PHASE": float(r["PHASE"] or 0)}
        cmp = _compare_constants(
            {n: {"FREQUENCY": r["FREQUENCY"], "AMPLITUDE": r["AMPLITUDE"], "PHASE": r["PHASE"]}
             for n, r in new.items()}, old)
        results.append((i, k, cmp))
        updated = max((r["LASTUPDATETIME"] or datetime.min) for r in g)
        z0 = old.get("Z0", {}).get("AMPLITUDE")
        z0s = f"{z0 * 1000:8.1f}" if z0 is not None else f"{'-':>8}"
        dup = f"（{len(g)} 列，名稱重複）" if len(g) != len(old) else ""
        lines.append(f" {i:>2}  {_pad(_pred_label(*k), 23)} {len(old):>6}{dup} {z0s}  "
                     f"{updated:%Y-%m-%d %H:%M}  {_verdict(cmp)}")

    # 各組差異明細；內容相同的組合併成一段（例如基準調整時每組都只差同樣的 Z0）
    details: dict[str, list[int]] = {}
    for i, k, cmp in results:
        if cmp["kind"] != "same":
            details.setdefault(_detail_text(cmp, len(new)), []).append(i)
    for text, ids in details.items():
        label = "、".join(f"#{i}" for i in ids)
        lines.append(f"\n{label} 差異：\n{text}")

    conflicts = [k for _, k, cmp in results if cmp["kind"] != "same"]
    affected = [k for k in conflicts if k[0] is not None]
    if affected:
        lines.append("\n受影響的既有預報（以舊常數產製，可能需以 --predict-only 重做）：")
        for pb, pe in affected:
            lines.append(f"  QC='{ah}' {pb}~{pe}（tide6ha、tideh）")
    return "\n".join(lines), conflicts


def _detail_text(cmp: dict, n_new: int) -> str:
    out = []
    if cmp["missing"] or cmp["added"]:
        out.append(f"  分潮名單：本次 {n_new} 個，既有 {cmp['n_old']} 個")
        if cmp["missing"]:
            out.append(f"    本次沒有：{', '.join(cmp['missing'])}")
        if cmp["added"]:
            out.append(f"    本次多出：{', '.join(cmp['added'])}")
    if cmp["dz0"] is not None:
        out.append(f"  Z0：{cmp['dz0'] * 1000:+.1f} mm" if abs(cmp["dz0"]) > _TOL_A else "  Z0：相同")
    if cmp["changed"]:
        out.append(f"  共同分潮 {cmp['n_common']} 個中有 {len(cmp['changed'])} 個不同，"
                   f"預報差異上限 {cmp['sum_dv'] * 1000:.1f} mm（各分潮向量差加總）；影響較大者：")
        for n, da, dg, amp, dv in cmp["changed"][:5]:
            out.append(f"    {n:<6} A {amp * 1000:7.1f} mm  ΔA {da * 1000:+6.1f} mm  "
                       f"ΔG {dg:+7.2f}°  向量差 {dv * 1000:5.1f} mm")
    else:
        out.append(f"  其餘 {cmp['n_common']} 個共同分潮：|ΔA| 最大 {cmp['max_da'] * 1000:.1f} mm、"
                   f"|ΔG| 最大 {cmp['max_dg']:.2f}°（振幅 ≥ 5 mm 者）")
    return "\n".join(out)


def _summary(consts: dict[str, dict]) -> str:
    z0 = consts.get("Z0", {}).get("AMPLITUDE")
    m2 = consts.get("M2")
    s = f"{len(consts)} 個分潮"
    if z0 is not None:
        s += f"  Z0={float(z0) * 1000:.1f} mm"
    if m2:
        s += f"  M2=A {float(m2['AMPLITUDE']) * 1000:.1f} mm / G {float(m2['PHASE']):.2f}°"
    return s


def _verdict(cmp: dict) -> str:
    if cmp["kind"] == "same":
        return "相同"
    if cmp["kind"] == "z0":
        return f"只有 Z0 不同（{cmp['dz0'] * 1000:+.1f} mm）→ 疑似基準調整"
    parts = []
    if cmp["missing"] or cmp["added"]:
        parts.append("分潮名單不同")
    if cmp["changed"]:
        parts.append(f"{len(cmp['changed'])} 個分潮數值不同（預報差異 ≤ {cmp['sum_dv'] * 1000:.1f} mm）")
    if cmp["dz0"] is not None and abs(cmp["dz0"]) > _TOL_A:
        parts.append(f"Z0 {cmp['dz0'] * 1000:+.1f} mm")
    return "、".join(parts) + " → 資料或方法有變動"


def list_tidesub(conn, stid: str) -> str:
    """
    某站 tidesub 全部分組總覽（唯讀）：每組一行，含分潮數、Z0、M2 與最後更新時間。
    同一 (ah, 分析期間) 底下各組的 Z0 或 M2 不一致時，於行首標 '≠'。
    """
    table = ENV["output_sub_table"]
    cur = conn.cursor(dictionary=True)
    cur.execute(
        f"""SELECT ah, DATEBEG, DATEEND, predbeg, predend,
                   COUNT(*) AS n, COUNT(DISTINCT NAME) AS n_name,
                   MAX(LASTUPDATETIME) AS updated,
                   MAX(CASE WHEN TRIM(NAME) = 'Z0' THEN AMPLITUDE END) AS z0,
                   MAX(CASE WHEN TRIM(NAME) = 'M2' THEN AMPLITUDE END) AS m2a,
                   MAX(CASE WHEN TRIM(NAME) = 'M2' THEN PHASE END) AS m2g
            FROM {table}
            WHERE STID = %s
            GROUP BY ah, DATEBEG, DATEEND, predbeg, predend
            ORDER BY DATEBEG, DATEEND, ah, predbeg""",
        (stid,),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return f"{table} 中沒有 STID={stid} 的資料。"

    # 同一 (ah, 分析期間) 各組的 Z0、M2 是否一致（容許誤差同 check_tidesub）
    vals: dict[tuple, list] = {}
    for r in rows:
        vals.setdefault((r["ah"], r["DATEBEG"], r["DATEEND"]), []).append(
            tuple(float(v) if v is not None else None for v in (r["z0"], r["m2a"], r["m2g"])))

    def differs(vs):
        for i, tol in ((0, _TOL_A), (1, _TOL_A), (2, _TOL_G)):
            xs = [v[i] for v in vs if v[i] is not None]
            if xs and max(xs) - min(xs) > tol:
                return True
        return False

    sig = {k: differs(v) for k, v in vals.items()}

    def mm(v):
        return f"{float(v) * 1000:7.1f}" if v is not None else f"{'-':>7}"

    out = [f"[tidesub 總覽] STID={stid}（{table}）共 {len(rows)} 組",
           f"   {'ah':<2} {_pad('分析期間', 21)}  {_pad('預報期間', 21)} {_pad('分潮數', 6)} "
           f"{'Z0(mm)':>7} {'M2 A(mm)':>8} {'M2 G':>7}  最後更新"]
    for r in rows:
        mark = "≠" if sig[(r["ah"], r["DATEBEG"], r["DATEEND"])] else " "
        n = f"{r['n_name']}" + (f"/{r['n']}列" if r["n"] != r["n_name"] else "")
        m2g = f"{float(r['m2g']):7.2f}" if r["m2g"] is not None else f"{'-':>7}"
        upd = f"{r['updated']:%Y-%m-%d %H:%M}" if r["updated"] else "-"
        out.append(f" {mark} {r['ah'] or '-':<2} {r['DATEBEG']}~{r['DATEEND']}  "
                   f"{_pad(_pred_label(r['predbeg'], r['predend']), 21)} {n:>6} "
                   f"{mm(r['z0'])} {mm(r['m2a']):>8} {m2g}  {upd}")
    if any(sig.values()):
        out.append("≠：同一 ah 與分析期間底下，各組 Z0 或 M2 不一致")
    return "\n".join(out)


# ─────────────────────────────────────────────
#  寫入
# ─────────────────────────────────────────────

# tide6ha 的 MIN0~MIN9 對應的分鐘
_TARGET_MINUTES = [0, 6, 12, 18, 24, 30, 36, 42, 48, 54]


def _day_start(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time())


def write_tide6ha(conn, minute_records: list[dict],
                  stid: str, stid_new: str | None, qc: str, batch_size: int = 500):
    """
    逐分鐘預報 [{dt, height_mm}] → tide6ha（每小時一列，MIN0~MIN9 = 00/06/.../54 分）。
    upsert，不 commit（由 write_outputs 統一提交）。
    PK = (STID, YEAR, MONTH, DAY, HOUR, QC)；預報期間內每個整點都會被覆寫，不會殘留舊資料。
    """
    minute_map = {(r["dt"].year, r["dt"].month, r["dt"].day, r["dt"].hour, r["dt"].minute):
                  r["height_mm"] for r in minute_records}
    hours = sorted({k[:4] for k in minute_map})
    rows = []
    for (y, m, d, h) in hours:
        vals = [minute_map.get((y, m, d, h, mn)) for mn in _TARGET_MINUTES]
        row = {"STID": stid, "STID_NEW": stid_new, "DATATIME": datetime(y, m, d, h),
               "YEAR": y, "MONTH": m, "DAY": d, "HOUR": h, "QC": qc, "FB": None}
        row.update({f"MIN{i}": v for i, v in enumerate(vals)})
        rows.append(row)

    mins = ", ".join(f"MIN{i}" for i in range(10))
    sql = f"""
        INSERT INTO {ENV['output_6ha_table']}
            (STID, STID_NEW, DATATIME, YEAR, MONTH, DAY, HOUR, {mins}, QC, FB)
        VALUES
            (%(STID)s, %(STID_NEW)s, %(DATATIME)s, %(YEAR)s, %(MONTH)s, %(DAY)s, %(HOUR)s,
             {", ".join(f"%(MIN{i})s" for i in range(10))}, %(QC)s, %(FB)s)
        ON DUPLICATE KEY UPDATE
            STID_NEW = VALUES(STID_NEW),
            DATATIME = VALUES(DATATIME),
            {", ".join(f"MIN{i} = VALUES(MIN{i})" for i in range(10))},
            LASTUPDATETIME = CURRENT_TIMESTAMP
    """
    _executemany(conn, sql, rows, batch_size)
    log.info(f"  tide6ha：{len(rows)} 列（每小時一列，QC='{qc}'）")


def write_tide1ha(conn, minute_records: list[dict],
                  stid: str, stid_new: str | None, qc: str, batch_size: int = 5000):
    """
    逐分鐘預報 [{dt, height_mm}] → tide1ha（每分鐘一列）。
    upsert，每個月 commit 一次（一年約 52 萬列，不放在單一交易）。
    PK = (STID, DATATIME, QC)；預報期間內每分鐘都會被覆寫，不會殘留舊資料。
    """
    sql = f"""
        INSERT INTO {ENV['output_1ha_table']}
            (STID, STID_NEW, DATATIME, Height, QC)
        VALUES
            (%(STID)s, %(STID_NEW)s, %(DATATIME)s, %(Height)s, %(QC)s)
        ON DUPLICATE KEY UPDATE
            STID_NEW = VALUES(STID_NEW),
            Height   = VALUES(Height),
            LASTUPDATETIME = CURRENT_TIMESTAMP
    """
    months: dict[tuple, list[dict]] = {}
    for r in minute_records:
        months.setdefault((r["dt"].year, r["dt"].month), []).append(
            {"STID": stid, "STID_NEW": stid_new, "DATATIME": r["dt"],
             "Height": r["height_mm"], "QC": qc})
    total = 0
    try:
        for (y, m), rows in sorted(months.items()):
            _executemany(conn, sql, rows, batch_size)
            conn.commit()
            total += len(rows)
            log.info(f"  tide1ha：{y}-{m:02d} {len(rows)} 列已提交（累計 {total}/{len(minute_records)}）")
    except Exception:
        conn.rollback()
        raise
    log.info(f"[DB寫入完成] tide1ha 共 {total} 列（QC='{qc}'）")


def replace_tideh(conn, records: list[dict], stid: str, qc: str,
                  pred_start: date, pred_end: date):
    """
    高低潮 → tideh：先刪 (STID, QC, 預報期間) 再插入，不 commit（由 write_outputs 統一提交）。
    PK 含分鐘，用 upsert 的話，時刻變動的舊事件會殘留，所以整段替換。
    records 需已篩在預報期間內。
    """
    table = ENV["output_table"]
    cur = conn.cursor()
    cur.execute(
        f"DELETE FROM {table} WHERE STID = %s AND QC = %s AND DATATIME >= %s AND DATATIME < %s",
        (stid, qc, _day_start(pred_start), _day_start(pred_end + timedelta(days=1))),
    )
    deleted = cur.rowcount
    cur.close()
    sql = f"""
        INSERT INTO {table}
            (STID, STID_NEW, DATATIME, YEAR, MONTH, DAY, HOUR, MINUTE, HORL, HEIGHT, QC, sn)
        VALUES
            (%(STID)s, %(STID_NEW)s, %(DATATIME)s, %(YEAR)s, %(MONTH)s, %(DAY)s,
             %(HOUR)s, %(MINUTE)s, %(HORL)s, %(HEIGHT)s, %(QC)s, %(sn)s)
    """
    _executemany(conn, sql, records, 500)
    log.info(f"  tideh：刪除舊資料 {deleted} 筆，寫入 {len(records)} 筆（QC='{qc}' {pred_start}~{pred_end}）")


def write_tidesub(conn, constants: list[dict], stid: str, stid_new: str | None,
                  datebeg: date, dateend: date, ah: str,
                  predbeg: date | None, predend: date | None,
                  supersede: list[tuple] = ()):
    """
    分潮常數 → tidesub：先刪同組 (STID, DATEBEG, DATEEND, ah, predbeg, predend)，
    以及 supersede 指定的其他組（--supersede 取代的舊常數），再插入。不 commit。

    不依賴 PK（.71 的 tidesub 沒有 PK）。未預報時 predbeg/predend 寫 NULL，
    比對用 <=>（NULL 安全等號，MySQL 5.7 可用）。
    ah='c' 由外部匯入，本程式不寫。
    """
    if ah == "c":
        log.warning("ah='c' 由外部匯入，跳過 tidesub 寫入。")
        return
    table = ENV["output_sub_table"]
    cur = conn.cursor()
    del_sql = f"""
        DELETE FROM {table}
        WHERE STID = %s AND DATEBEG = %s AND DATEEND = %s AND ah = %s
          AND predbeg <=> %s AND predend <=> %s
    """
    deleted = 0
    for pb, pe in [(predbeg, predend)] + [k for k in supersede if k != (predbeg, predend)]:
        cur.execute(del_sql, (stid, datebeg, dateend, ah, pb, pe))
        deleted += cur.rowcount
    cur.close()

    rows = [
        {"STID": stid, "STID_NEW": stid_new, "DATEBEG": datebeg, "DATEEND": dateend,
         "NAME": r["NAME"], "FREQUENCY": r["FREQUENCY"],
         "AMPLITUDE": r["AMPLITUDE"], "PHASE": r["PHASE"],
         "APLITUDE": r.get("APLITUDE"), "PHASEL": r.get("PHASEL"),
         "ah": ah, "predbeg": predbeg, "predend": predend}
        for r in constants
    ]
    sql = f"""
        INSERT INTO {table}
            (STID, STID_NEW, DATEBEG, DATEEND, NAME, FREQUENCY,
             AMPLITUDE, PHASE, APLITUDE, PHASEL, ah, predbeg, predend)
        VALUES
            (%(STID)s, %(STID_NEW)s, %(DATEBEG)s, %(DATEEND)s, %(NAME)s, %(FREQUENCY)s,
             %(AMPLITUDE)s, %(PHASE)s, %(APLITUDE)s, %(PHASEL)s,
             %(ah)s, %(predbeg)s, %(predend)s)
    """
    _executemany(conn, sql, rows, 500)
    log.info(f"  tidesub：刪除舊資料 {deleted} 筆，寫入 {len(rows)} 個分潮"
             f"（ah='{ah}' {datebeg}~{dateend}，預報 {_pred_label(predbeg, predend)}）")


def write_outputs(conn, *, stid: str, stid_new: str | None, flag: str,
                  pred_start: date | None, pred_end: date | None,
                  minute_records: list[dict] | None,
                  extreme_records: list[dict] | None,
                  constants: list[dict] | None,
                  datebeg: date | None, dateend: date | None,
                  supersede: list[tuple] = ()):
    """
    依 flag 寫入（兩個引擎共用的分流規則）：
      flag c   ：逐分鐘 → tide1ha（每月 commit）
      flag a/h ：逐分鐘 → tide6ha、高低潮 → tideh、常數 → tidesub，同一交易內 commit；
                 任何一步失敗全部 rollback，DB 維持原狀。
    """
    if flag == "c":
        if minute_records:
            write_tide1ha(conn, minute_records, stid, stid_new, flag)
        else:
            log.warning("無逐分鐘預報可寫入 tide1ha。")
        return

    try:
        if minute_records:
            write_tide6ha(conn, minute_records, stid, stid_new, flag)
        elif pred_start is not None:
            log.warning("無逐分鐘預報可寫入 tide6ha。")
        if pred_start is not None:
            if extreme_records:
                replace_tideh(conn, extreme_records, stid, flag, pred_start, pred_end)
            else:
                log.warning("無高低潮可寫入 tideh，保留既有資料不刪除。")
        if constants:
            write_tidesub(conn, constants, stid, stid_new, datebeg, dateend,
                          flag, pred_start, pred_end, supersede)
        conn.commit()
    except Exception:
        conn.rollback()
        log.error("寫入失敗，已 rollback，本次未變動任何資料表。")
        raise
    log.info("[DB寫入完成] 已提交（commit）")


def _executemany(conn, sql: str, rows: list[dict], batch_size: int):
    cur = conn.cursor()
    try:
        for i in range(0, len(rows), batch_size):
            cur.executemany(sql, rows[i:i + batch_size])
    finally:
        cur.close()


def apply_slack_tide_marking(records: list[dict], threshold_mm: int = 40) -> list[dict]:
    """
    相鄰高低潮事件（依 DATATIME 排序後的前後兩筆）水位差小於 threshold_mm，
    兩者皆標記為 'S'（平潮）。threshold_mm 預設 40（= 4 公分，與 tideh.HORL 欄位註解一致）。
    """
    records_sorted = sorted(records, key=lambda r: r["DATATIME"])
    for i in range(len(records_sorted) - 1):
        h1 = records_sorted[i]["HEIGHT"]
        h2 = records_sorted[i + 1]["HEIGHT"]
        if h1 is None or h2 is None:
            continue
        if abs(h1 - h2) < threshold_mm:
            records_sorted[i]["HORL"] = "S"
            records_sorted[i + 1]["HORL"] = "S"
    return records_sorted


# ─────────────────────────────────────────────
#  排程用：自動挑選要跑的站（cron_tide.sh 呼叫）
# ─────────────────────────────────────────────

# 站點表篩選條件（SQL WHERE），.env 的 TIDE_STATION_FILTER 可覆寫
STATION_FILTER = os.environ.get("TIDE_STATION_FILTER") or "kind = '6' AND reliable = '1'"
# 分析期間 qc='Q' 整點資料至少要佔的比例，.env 的 TIDE_MIN_COVERAGE 可覆寫
MIN_COVERAGE = float(os.environ.get("TIDE_MIN_COVERAGE") or 0.8)


def scheduled_stations(conn, flag: str, start: date | None = None, end: date | None = None,
                       min_coverage: float = MIN_COVERAGE) -> tuple[list[str], list[str]]:
    """
    回傳 (要跑的站碼, 略過的站與原因)。
      flag a/h：站點表符合 STATION_FILTER，且分析期間 qc='Q' 整點資料比例 ≥ min_coverage
      flag c  ：tidesub 有 ah='c' 常數的站（常數是外部刻意匯入的，停測站也照樣預報）
    """
    cur = conn.cursor()
    if flag == "c":
        cur.execute(f"SELECT DISTINCT STID FROM {ENV['const_c_table']} WHERE ah = 'c' ORDER BY STID")
        stations = [r[0] for r in cur.fetchall()]
        cur.close()
        return stations, []

    cur.execute(f"SELECT stid, stnac FROM {ENV['station_table']} WHERE {STATION_FILTER} ORDER BY stid")
    candidates = cur.fetchall()
    cur.execute(
        f"""SELECT STID, COUNT(*) FROM {ENV['water_table']}
            WHERE qc = 'Q' AND DATATIME >= %s AND DATATIME < %s
            GROUP BY STID""",
        (_day_start(start), _day_start(end + timedelta(days=1))),
    )
    counts = dict(cur.fetchall())
    cur.close()
    hours = ((end - start).days + 1) * 24
    run, skipped = [], []
    for stid, name in candidates:
        cov = counts.get(stid, 0) / hours
        if cov >= min_coverage:
            run.append(stid)
        else:
            skipped.append(f"{stid} {name}：{start}~{end} 有效整點資料 {cov:.0%} < {min_coverage:.0%}")
    return run, skipped


if __name__ == "__main__":
    # python tide_db.py stations --env prod --flag h --start 2024-01-01 --end 2025-12-31
    # stdout 每行「站碼<TAB>中文站名」（cron_tide.sh 只取第一欄）；略過的站與原因寫到 stderr（cron_tide.sh 收進 log）
    import argparse
    import sys
    p = argparse.ArgumentParser(description="列出排程要跑的站")
    p.add_argument("command", choices=["stations"])
    p.add_argument("--env", choices=list(ENVIRONMENTS), default="local")
    p.add_argument("--flag", choices=["a", "h", "c"], required=True)
    p.add_argument("--start")
    p.add_argument("--end")
    a = p.parse_args()
    if a.flag != "c" and not (a.start and a.end):
        sys.exit("flag a/h 需要 --start/--end（用來計算資料比例）")
    select_env(a.env)
    conn = get_conn()
    try:
        run, skipped = scheduled_stations(
            conn, a.flag,
            date.fromisoformat(a.start) if a.start else None,
            date.fromisoformat(a.end) if a.end else None)
        cur = conn.cursor()
        cur.execute(f"SELECT stid, stnac FROM {ENV['station_table']}")
        names = {r[0]: (r[1] or "").strip() for r in cur.fetchall()}
        cur.close()
    finally:
        conn.close()
    print(f"站點篩選：{STATION_FILTER if a.flag != 'c' else 'tidesub 有 c 常數'}；"
          f"執行 {len(run)} 站、略過 {len(skipped)} 站", file=sys.stderr)
    for s in skipped:
        print(f"  略過 {s}", file=sys.stderr)
    for s in run:
        print(f"{s}\t{names.get(s, '')}")
