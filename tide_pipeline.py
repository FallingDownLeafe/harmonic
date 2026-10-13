#!/usr/bin/env python3
"""
tide_pipeline.py  —  Part 1 / 3
台灣潮汐 Fortran 四步驟預報流程 Python 包裝腳本

流程：
  分析（flag a/h）：tide6 讀水位 → trans1b / TIDE1_R2kb → sta1-gmt.har → 寫入 tidesub
  預報：sta1-gmt.har（分析產出，或由 tidesub 常數組出）→ trans2 / TIDE4R2b
        → sta1-prd1-gmt.dat → tide6ha（flag a/h）或 tide1ha（flag c）
        → sta1-prd2-gmt.dat → tideh（flag a/h）

執行方式：
  # 只分析（不給預報期間）
  python tide_pipeline.py --stid 1516 --start 2024-01-01 --end 2025-12-31 --flag h
  # 分析 + 預報
  python tide_pipeline.py --stid 1516 --start 2024-01-01 --end 2025-12-31 \
                          --flag h --predbeg 2026-01-01 --predend 2026-12-31
  # 只預報：用 tidesub 中 (STID, ah, DATEBEG=--start, DATEEND=--end) 的常數
  python tide_pipeline.py --stid 1516 --start 2024-01-01 --end 2025-12-31 \
                          --flag h --predbeg 2026-01-01 --predend 2026-12-31 --predict-only
  # 最佳常數預報：用 tidesub 中 ah='c' 的常數，只寫 tide1ha
  python tide_pipeline.py --stid 1516 --flag c --predbeg 2026-01-01 --predend 2026-12-31
  # 列出某站 tidesub 全部分組（唯讀）
  python tide_pipeline.py --stid 1516 --list-tidesub

分析模式寫入前會比對 tidesub 同一分析期間的既有常數；不同時（例如水位基準調整後重算）
印出報告並停止，確認後加 --supersede 重跑即以本次結果取代。DB 讀寫在 tide_db.py。

依賴：
  pip install mysql-connector-python
  --runner docker（預設）：Docker 容器 tidetest 需在執行前已啟動，/work 掛載至工作目錄
  --runner native：直接執行 TIDE_BIN_DIR 下的執行檔（Linux 部署用預先編譯版，見 deploy/README.md）
  路徑預設皆相對於本腳本目錄，可由 .env 的 TIDE_WORK_DIR / TIDE_BIN_DIR / TIDE_RUNNER 覆寫
"""

import argparse
import logging
import os
import re
import shutil
import subprocess
import sys
import textwrap
from datetime import date, datetime, timedelta
from pathlib import Path

# 資料庫層（設定、讀寫）放在 tide_db.py，與 UTide 流程共用；
# 以下名稱在此重新匯出，維持 tide_pipeline.fetch_station_meta 等舊用法
from tide_db import (
    BASE_DIR, ENV, ENVIRONMENTS, select_env, get_conn,
    fetch_station_meta, fetch_water_levels, fetch_tidesub_constants, parse_lat_lon_from_meta,
    check_tidesub, list_tidesub, write_outputs, apply_slack_tide_marking,
)

# ─────────────────────────────────────────────
#  設定區：依環境修改
# ─────────────────────────────────────────────

# Docker 容器名稱
DOCKER_CONTAINER = "tidetest"

# 本地工作目錄（docker 模式需與容器 /work 掛載一致）；.env 的 TIDE_WORK_DIR 或 --work-dir 可覆寫
def _dir_from_env(name: str, default: Path) -> Path:
    """讀取目錄型環境變數；相對路徑以 BASE_DIR 為基準，未設定則用 default。"""
    value = os.environ.get(name)
    return BASE_DIR / value if value else default


WORK_DIR = _dir_from_env("TIDE_WORK_DIR", BASE_DIR / "workdir")

# Fortran 執行方式：docker（容器 tidetest）或 native（本機執行）；
# 預設取 .env 的 TIDE_RUNNER，main() 再依 --runner 覆寫
RUNNER = os.environ.get("TIDE_RUNNER") or "docker"

# native 模式使用的 gfortran（PATH 上的指令名或完整路徑），可由 .env 的 GFORTRAN 指定
GFORTRAN = os.environ.get("GFORTRAN") or "gfortran"

# native 模式的執行檔目錄（編譯輸出與執行都用這裡）；部署時放預先編譯好的執行檔，
# 由 .env 的 TIDE_BIN_DIR 指定，預設與工作目錄相同
FORTRAN_BIN_DIR = _dir_from_env("TIDE_BIN_DIR", WORK_DIR)

# Fortran 原始碼所在目錄（Docker 容器內路徑）
FORTRAN_SRC_DIR = "/work/src"

# Fortran 原始碼所在目錄（native 模式，本機路徑）
FORTRAN_SRC_DIR_NATIVE = BASE_DIR / "workdir" / "src"

# 靜態輸入檔（每次執行前複製到工作目錄）
TIDE2_HDR_PATH = BASE_DIR / "tide2.hdr"   # 6 行天文常數表頭
TIDE3_DAT_PATH = BASE_DIR / "tide3.dat"   # Foreman 分潮常數資料庫，368行固定
TIDE5_HDR_PATH = BASE_DIR / "tide5.hdr"   # 219 行天文常數

# sta1-infs1.txt 中的推算分潮設定（若無推算留空字串）
# 格式：每行 "CONST1  CONST2  ratio  phase" 依 trans2 需求填入
INFERENCE_LINES = ""   # 例："SA      S2      0.0    0.0\n"

# 預報結果 QC 標記（預設值；main() 會以 --flag 覆寫）
FORECAST_QC = "h"

# 2位年 → 4位年門檻：<70 → 2000s，>=70 → 1900s（parse_prd1 / parse_prd2 共用）
_CENTURY_THRESHOLD = 70

# ─────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  引數解析
# ─────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="台灣潮汐 Fortran 四步驟預報流程 Python 包裝腳本"
    )
    p.add_argument("--stid", required=True,
                   help="站碼，例如 1516")
    p.add_argument("--start", default=None,
                   help="分析起始日 YYYY-MM-DD（→ tidesub.DATEBEG）；flag a/h 必填，"
                        "--predict-only 時用來指定沿用哪次分析的常數；flag c 不使用")
    p.add_argument("--end", default=None,
                   help="分析結束日 YYYY-MM-DD（→ tidesub.DATEEND）；用法同 --start")
    p.add_argument("--flag", choices=["a", "h", "c"], default="h",
                   help=(
                       "分潮標記，同時設定 tidesub.ah / tide6ha.QC / tide1ha.QC / tideh.QC。"
                       "a=調和分析，h=調和預報（預設）：分析寫 tidesub，預報寫 tide6ha + tideh；"
                       "c=最佳常數：讀 tidesub 中 ah='c' 的常數預報，只寫 tide1ha"
                   ))
    p.add_argument("--predbeg", default=None,
                   help=(
                       "預報起始日 YYYY-MM-DD（需與 --predend 同時提供），決定 Fortran 預報期間（sta1-bnd1.txt）"
                       "與 tideh / tide6ha / tide1ha 寫入範圍，"
                       "並寫入 tidesub.predbeg。省略時不預報（只寫 tidesub），"
                       "tidesub.predbeg 寫入 NULL。"
                   ))
    p.add_argument("--predend", default=None,
                   help=(
                       "預報結束日 YYYY-MM-DD（需與 --predbeg 同時提供），用途同 --predbeg，"
                       "並寫入 tidesub.predend。省略時不預報（只寫 tidesub），"
                       "tidesub.predend 寫入 NULL。"
                   ))
    p.add_argument("--predict-only", action="store_true",
                   help="flag a/h：不重新分析，改讀 tidesub 中 (STID, ah, DATEBEG=--start, "
                        "DATEEND=--end) 的常數來預報，並以本次預報期間另存一份 tidesub 紀錄。"
                        "需搭配 --predbeg/--predend")
    p.add_argument("--supersede", action="store_true",
                   help="分析模式：tidesub 同一分析期間已有不同的常數時（報告會列出），"
                        "以本次結果取代那些舊組；未加時遇到不同會停止、不寫入")
    p.add_argument("--list-tidesub", action="store_true",
                   help="只列出 --stid 在 tidesub 的全部分組（分潮數、Z0、M2、最後更新），不執行流程")
    p.add_argument("--work-dir",
                   help=f"本地工作目錄（預設 {WORK_DIR}）",
                   default=None)
    p.add_argument("--no-compile", action="store_true",
                   help="跳過 gfortran 編譯（假設可執行檔已存在）")
    p.add_argument("--dry-run", action="store_true",
                   help="只組裝輸入檔，不執行 Fortran 也不寫入 DB")
    p.add_argument("--skip-db-write", action="store_true",
                   help="執行 Fortran 但不寫入 DB")
    p.add_argument("--stid-new",
                   help="STID_NEW（新站碼，選填）",
                   default=None)
    p.add_argument("--env",
               choices=["local", "mirror", "prod"],
               default="local",
               help="執行環境（預設 local）")
    p.add_argument("--runner",
                   choices=["docker", "native"],
                   default=RUNNER,
                   help=f"Fortran 執行方式（預設 {RUNNER}，可由 .env 的 TIDE_RUNNER 設定）："
                        "docker=容器 tidetest；native=本機執行 TIDE_BIN_DIR 下的執行檔"
                        "（找不到 gfortran 時直接使用預先編譯好的執行檔）")
    return p.parse_args()


# ─────────────────────────────────────────────
#  Fortran 輸入檔組裝：sta1-local.txt
# ─────────────────────────────────────────────

def build_sta1_local(
    work_dir: Path,
    stid: str,
    lat_deg: int, lat_min: int,
    lon_deg: int, lon_min: int,
    daily_data: list[dict],
) -> Path:
    """
    組裝 sta1-local.txt（trans1b 的主要輸入）。

    格式：
      第 1 行：lat_deg lat_min lon_deg lon_min  （自由格式整數）
      第 2 行：站碼（最多 6 字元）
      之後每行：year month day h1 h2 ... h24
                水位單位毫米整數，缺值 9999
    """
    out_path = work_dir / "sta1-local.txt"
    lines = []

    # 第 1 行：座標
    lines.append(f"{lat_deg} {lat_min} {lon_deg} {lon_min}")

    # 第 2 行：站碼（左對齊，最多 6 碼）
    lines.append(stid[:6])

    # 資料行
    for day in daily_data:
        wl_str = " ".join(str(h) for h in day["hours"])
        lines.append(f"{day['year']} {day['month']} {day['day']} {wl_str}")

    content = "\n".join(lines) + "\n"
    out_path.write_text(content, encoding="ascii")
    log.info(f"已寫入 {out_path}（{len(daily_data)} 天）")
    return out_path


# ─────────────────────────────────────────────
#  組裝 sta1-infs1.txt
# ─────────────────────────────────────────────

def build_sta1_infs1(
    work_dir: Path,
    lat_deg: int, lat_min: int,
    lon_deg: int, lon_min: int,
    stid: str,
) -> Path:
    """
    組裝 sta1-infs1.txt（trans2 讀取的站點/推算資訊）。

    格式（由 trans1b FORMAT 410/420 確認）：
      第 1 行：I2,1X,I2,4X,I3,2X,I2  → "LD LM   LDD  LM"
      第 2 行：A6 → 站碼
    """
    out_path = work_dir / "sta1-infs1.txt"
    # FORMAT 410: i2,1x,i2,4x,i3,2x,i2
    line1 = f"{lat_deg:2d} {lat_min:2d}    {lon_deg:3d}  {lon_min:2d}"
    line2 = f"{stid:<6s}"
    content = line1 + "\n" + line2 + "\n"
    if INFERENCE_LINES:
        content += INFERENCE_LINES
    out_path.write_text(content, encoding="ascii")
    log.info(f"已寫入 {out_path}")
    return out_path


# ─────────────────────────────────────────────
#  組裝 sta1-bnd1.txt
# ─────────────────────────────────────────────

def build_sta1_bnd1(
    work_dir: Path,
    start_date: date,
    end_date: date,
    ibad: int = 0,
) -> Path:
    """
    組裝 sta1-bnd1.txt（trans2 讀取的預報期間）。

    格式：
      第 1 行：起始年 月 日  結束年 月 日
      第 2 行：ibad（0 表示正常）

    注意：trans2 SVN 版本的 beforeday/afterday 副程式
          會在此基礎上各加減一天，Python 端不需要手動偏移。
    """
    out_path = work_dir / "sta1-bnd1.txt"
    line1 = (
        f"{start_date.year} {start_date.month:2d} {start_date.day:2d} "
        f"{end_date.year} {end_date.month:2d} {end_date.day:2d}"
    )
    content = line1 + "\n" + str(ibad) + "\n"
    out_path.write_text(content, encoding="ascii")
    log.info(f"已寫入 {out_path}（{start_date} ~ {end_date}）")
    return out_path


# ─────────────────────────────────────────────
#  複製靜態輸入檔（tide2.hdr / tide5.hdr）
# ─────────────────────────────────────────────

def copy_static_files(work_dir: Path):
    """將 tide2.hdr / tide5.hdr 複製到工作目錄（若來源與目的不同）。"""
    for src, dst_name in [
        (TIDE2_HDR_PATH, "tide2.hdr"),
        (TIDE3_DAT_PATH, "tide3.dat"),   # ← 新增這一行
        (TIDE5_HDR_PATH, "tide5.hdr"),
    ]:
        dst = work_dir / dst_name
        if not src.exists():
            raise FileNotFoundError(f"找不到靜態檔案：{src}")
        # 若來源與目的相同則跳過，避免 Windows 鎖定問題
        if src.resolve() != dst.resolve():
            shutil.copy2(src, dst)
            log.info(f"已複製 {src} → {dst}")
        else:
            log.info(f"靜態檔案已在工作目錄：{dst}（跳過複製）")



# ─────────────────────────────────────────────
#  Fortran 執行環境（--runner docker | native）
# ─────────────────────────────────────────────

def run_in_workdir(argv: list[str]) -> int:
    """
    在工作目錄下執行指令。
      docker：docker exec 至容器 DOCKER_CONTAINER 的 /work（= WORK_DIR 掛載點）
      native：直接在本機 WORK_DIR 執行；GFORTRAN 為完整路徑時，
              將其所在目錄加入 PATH，讓編譯出的執行檔找得到 runtime DLL
    回傳 return code；非零時拋出 RuntimeError。
    """
    if RUNNER == "docker":
        full_cmd = ["docker", "exec", "-w", "/work", DOCKER_CONTAINER, *argv]
        cwd, env = None, None
    else:
        full_cmd, cwd, env = argv, WORK_DIR, os.environ.copy()
        gfortran_dir = Path(GFORTRAN).parent
        if str(gfortran_dir) not in ("", "."):
            env["PATH"] = str(gfortran_dir) + os.pathsep + env.get("PATH", "")
    cmd = " ".join(argv)
    log.info(f"[{RUNNER}] {cmd}")
    result = subprocess.run(
        full_cmd,
        cwd=cwd,
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.debug(f"  [stdout] {line}")
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            log.warning(f"  [stderr] {line}")
    if result.returncode != 0:
        raise RuntimeError(
            f"{RUNNER} 指令失敗（code={result.returncode}）: {cmd}\n"
            f"stderr: {result.stderr[:500]}"
        )
    return result.returncode


def _exe_path(exe_name: str) -> str:
    """執行檔路徑：docker 用容器內相對路徑；native 在 Windows 加 .exe。"""
    if RUNNER == "docker":
        return f"./{exe_name}"
    suffix = ".exe" if os.name == "nt" else ""
    return str(FORTRAN_BIN_DIR / f"{exe_name}{suffix}")


def compile_fortran(src_name: str, exe_name: str, no_compile: bool = False):
    """
    編譯單支 Fortran 程式（gfortran -O2 固定格式）。
    若 no_compile=True 則跳過（假設可執行檔已存在）。
    native 模式下找不到 gfortran 但執行檔已存在時，直接沿用（部署環境用預先編譯的執行檔）。
    """
    if no_compile:
        log.info(f"[跳過編譯] {src_name}")
        return
    if RUNNER == "docker":
        compiler, src = "gfortran", f"{FORTRAN_SRC_DIR}/{src_name}"
    else:
        compiler, src = GFORTRAN, str(FORTRAN_SRC_DIR_NATIVE / src_name)
        if shutil.which(GFORTRAN) is None:
            if Path(_exe_path(exe_name)).exists():
                log.info(f"[未安裝 gfortran，使用既有執行檔] {_exe_path(exe_name)}")
                return
            raise RuntimeError(f"找不到 gfortran（{GFORTRAN}），也沒有既有執行檔 {_exe_path(exe_name)}")
    run_in_workdir([
        compiler, "-O2", "-ffixed-form", "-ffixed-line-length-none",
        # -std=legacy：忽略老舊程式碼 array overloading tricks 在 gfortran 8 後被升級成的錯誤
        # （-fallow-argument-mismatch 對此類錯誤無效）
        "-std=legacy",
        "-o", _exe_path(exe_name), src,
    ])
    log.info(f"[編譯完成] {src_name} → {exe_name}")


# ─────────────────────────────────────────────
#  四步驟 Fortran 執行
# ─────────────────────────────────────────────

def run_trans1b(no_compile: bool):
    """
    步驟 1：trans1b
      輸入：sta1-local.txt, tide2.hdr（→ tide2.dat 表頭）
      輸出：sta1-infs1.txt（座標/站碼）、tide2.dat（水位資料，即 sta1-gmt.txt）
    """
    compile_fortran("trans1b.for", "trans1b", no_compile)
    run_in_workdir([_exe_path("trans1b")])
    _check_log("trans1b.log", step="trans1b")
    log.info("[步驟1完成] trans1b → tide2.dat, sta1-infs1.txt")


def run_tide1_r2kb(no_compile: bool):
    """
    步驟 2：TIDE1_R2kb
      輸入：tide2.dat（= sta1-gmt.txt）、tide3.dat（= tide2.hdr 表頭的後半，
            含分潮資訊；需確認 tide3.dat 與 tide2.hdr 的對應關係）
      輸出：sta1-gmt.har（60個分潮調和常數）
    注意：TIDE1_R2kb 開啟 tide2.dat(unit=4) 和 tide3.dat(unit=8)。
          tide3.dat 通常是 Foreman 分潮常數資料庫，與 tide2.hdr 不同。
          請確認 tide3.dat 是否已存放於工作目錄。
    """
    compile_fortran("TIDE1_R2kb.for", "tide1_r2kb", no_compile)
    run_in_workdir([_exe_path("tide1_r2kb")])
    _check_log("tide1_r2kb.log", step="TIDE1_R2kb")
    log.info("[步驟2完成] TIDE1_R2kb → sta1-gmt.har")


def run_trans2(no_compile: bool):
    """
    步驟 3：trans2
      輸入：sta1-gmt.har, sta1-infs1.txt, sta1-bnd1.txt, tide5.hdr
      輸出：tide5.dat
    """
    compile_fortran("trans2.for", "trans2", no_compile)
    run_in_workdir([_exe_path("trans2")])
    log.info("[步驟3完成] trans2 → tide5.dat")


def run_tide4r2b(no_compile: bool):
    """
    步驟 4：TIDE4R2b
      輸入：tide5.dat
      輸出：sta1-prd1-gmt.dat（逐分鐘連續預報）、sta1-prd2-gmt.dat（高低潮）
    """
    compile_fortran("TIDE4R2b.for", "tide4r2b", no_compile)
    run_in_workdir([_exe_path("tide4r2b")])
    log.info("[步驟4完成] TIDE4R2b → sta1-prd1-gmt.dat, sta1-prd2-gmt.dat")


def _check_log(log_filename: str, step: str):
    """讀取 Fortran log 檔（docker 模式下 /work 即 WORK_DIR 掛載點），若有明顯錯誤字樣則警告。"""
    log_path = WORK_DIR / log_filename
    content = (log_path.read_text(encoding="utf-8", errors="replace").strip()
               if log_path.exists() else "")
    if content:
        log.info(f"[{step} log]\n{content}")
    # 常見錯誤關鍵字
    for kw in ("error", "Error", "ERROR", "less than 31", "interrupted too long"):
        if kw in content:
            raise RuntimeError(f"[{step}] Fortran log 含錯誤訊息：{content[:300]}")


# ─────────────────────────────────────────────
#  只預報：由 tidesub 常數組出 sta1-gmt.har
# ─────────────────────────────────────────────

def write_har_from_constants(work_dir: Path, stid: str, records: list[dict],
                             datebeg: date, dateend: date) -> Path:
    """
    以分潮常數寫出與 TIDE1_R2kb 輸出同版面的 sta1-gmt.har，供 trans2 讀取。

    trans2 從 '  NO' 表頭的下一行開始，原樣複製到空行為止；
    TIDE4R2b 以 FORMAT(5X,A5,28X,F8.4,F7.2) 只讀 NAME、A（AMPLITUDE）、G（PHASE），
    節點修正由 TIDE4R2b 依預報時間自行計算，AL/GL 僅供參考。
    """
    my = f"{datebeg.month}{datebeg.year % 100:02d}".rjust(4) + "/" + \
         f"{dateend.month}{dateend.year % 100:02d}".rjust(4)
    lines = [
        f" CONSTANTS FROM DB  STN {stid}  {datebeg} TO {dateend}",
        "",
        "  NO NAME    FREQUENCY   STN  M-Y/ M-Y       A      G                  AL     GL",
    ]
    for no, r in enumerate(records, start=1):
        if len(r["NAME"]) > 5:
            raise ValueError(f"分潮名稱超過 5 字元，Fortran 無法讀取：{r['NAME']}")
        head = f"{no:4d} {r['NAME']:<5}{r['FREQUENCY']:12.8f} {stid[:4]:>4}  {my}"
        # AL/GL 可能為 NULL（UTide 產製的常數），TIDE4R2b 不讀這兩欄，寫 0
        al, gl = r["APLITUDE"] or 0.0, r["PHASEL"] or 0.0
        lines.append(f"{head:<38}{r['AMPLITUDE']:8.4f}{r['PHASE']:7.2f}"
                     f"{'':12}{al:8.4f}{gl:7.2f}")
    lines.append("")
    out_path = work_dir / "sta1-gmt.har"
    out_path.write_text("\n".join(lines) + "\n", encoding="ascii")
    log.info(f"已由 tidesub 常數寫出 {out_path}（{len(records)} 個分潮）")
    return out_path


# ─────────────────────────────────────────────
#  解析 sta1-prd1-gmt.dat（逐分鐘連續預報）
# ─────────────────────────────────────────────

def parse_prd1(prd1_path: Path,
               pred_start: date | None = None,
               pred_end:   date | None = None) -> list[dict]:
    """
    解析 sta1-prd1-gmt.dat（TIDE4R2b 逐分鐘連續預報輸出）。
    回傳 list of {"dt": datetime, "height_mm": int}。

    格式（FORMAT(A6,F7.3,I3,2I2,8F6.3,F12.4)，必須用字元位置解析，
    不可用 split()——負值讀值與相鄰欄位之間無空格）：
      [0:6]   A6    站碼
      [6:13]  F7.3  t0，本行第一筆的起始小時（小數）
      [13:16] I3    日
      [16:18] I2    月
      [18:20] I2    2位年
      [20:68] 8×F6.3  8筆水位（公尺），各6字元，[20:26]/[26:32]/.../[62:68]
      [68:80] F12.4 TSPC，相鄰兩筆間隔小時數（≈0.0167 h ≈ 1 min）

    資料行判斷：line[0:4].strip().isdigit()（表頭行含字母，自然跳過）。

    時間計算：
      base_dt = datetime(year, month, day)
      dt_raw  = base_dt + timedelta(hours = t0 + i * tspc)    i = 0..7
      四捨五入至整分：seconds >= 30 → +1 min，然後歸零 seconds/microseconds。
      timedelta 自動處理跨日 rollover，不需手動取模。

    beforeday/afterday 邊界處理：
      trans2 固定在請求起訖各加減一天。beforeday 末行的第 8 筆
      （t0 + 7×TSPC ≈ 24.001 h）rollover 至 pred_start 00:00，
      通過 pred_start 篩選，是正確的首筆 00:00 預報值。
      其他 beforeday 各筆早於 pred_start，自動被過濾掉。

    pred_start / pred_end 為 None 時不對該端施加篩選。
    """
    if not prd1_path.exists():
        raise FileNotFoundError(f"找不到輸出檔：{prd1_path}")

    pred_start_dt = (
        datetime(pred_start.year, pred_start.month, pred_start.day, 0, 0, 0)
        if pred_start is not None else None
    )
    pred_end_dt = (
        datetime(pred_end.year, pred_end.month, pred_end.day, 23, 59, 59)
        if pred_end is not None else None
    )

    records: list[dict] = []

    with prd1_path.open("r", encoding="ascii", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            # 最短有效行：A6 + F7.3 + I3 + 2I2 + 8×F6.3 = 68 字元
            if len(line) < 68:
                continue
            # 資料行：[0:4] 為站碼數字（表頭行含 'STN' 等字母，自然不符）
            if not line[0:4].strip().isdigit():
                continue

            try:
                t0   = float(line[6:13])
                day  = int(line[13:16])
                mo   = int(line[16:18])
                yr2  = int(line[18:20])
                tspc = float(line[68:80]) if len(line) >= 80 else float(line[68:])
            except ValueError as e:
                log.warning(f"prd1 解析跳過行（格式異常）：{e}")
                continue

            year = (2000 + yr2) if yr2 < _CENTURY_THRESHOLD else (1900 + yr2)
            try:
                base_dt = datetime(year, mo, day)
            except ValueError as e:
                log.warning(f"prd1 無效日期 {year}-{mo}-{day}：{e}，跳過整行")
                continue

            for i in range(8):
                hgt_str = line[20 + i * 6 : 26 + i * 6]
                try:
                    hgt_m = float(hgt_str)
                except ValueError:
                    continue

                # 計算實際時間並四捨五入至整分
                dt_raw = base_dt + timedelta(hours=t0 + i * tspc)
                if dt_raw.second >= 30:
                    dt_raw += timedelta(minutes=1)
                dt = dt_raw.replace(second=0, microsecond=0)

                # 時間篩選
                if pred_start_dt is not None and dt < pred_start_dt:
                    continue
                if pred_end_dt is not None and dt > pred_end_dt:
                    continue

                records.append({
                    "dt":        dt,
                    "height_mm": round(hgt_m * 1000),
                })

    log.info(f"[解析完成] sta1-prd1-gmt.dat → {len(records)} 筆分鐘預報記錄")
    return records




# ─────────────────────────────────────────────
#  解析 sta1-prd2-gmt.dat（高低潮）
# ─────────────────────────────────────────────

def parse_prd2(prd2_path: Path, stid: str, stid_new: str | None,
               qc: str | None = None) -> list[dict]:
    """
    解析 sta1-prd2-gmt.dat，回傳 list of dict，每筆對應 tideh 一列。

    qc 參數：若傳入則使用，否則 fallback 至模組層級 FORECAST_QC。

    已確認欄位格式：
      [0]   : 空格（print control）
      [1]   : IC   1=高潮(H) 2=低潮(L)
      [2:8] : KSTN 站碼
      [8:10]: IDY  日
      [10:13]: MO  月（3碼）
      [13:15]: IYR 2位年
      [15:] : 最多 6 組高低潮，每組 11 碼：
                TIME(5碼) + HGT(6碼)
                TIME strip() == '9999' → 填充欄，跳過

    HEIGHT 單位：
      prd2 檔中 HGT 為公尺（如 0.418），tideh.HEIGHT 為毫米整數 → 乘以 1000 取整
    """
    if not prd2_path.exists():
        raise FileNotFoundError(f"找不到輸出檔：{prd2_path}")

    _qc = qc if qc is not None else FORECAST_QC

    records = []

    with prd2_path.open("r", encoding="ascii", errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")
            # 跳過表頭（含字母 'DATE' 或 'STN' 的行）
            if len(line) < 16:
                continue
            ic_char = line[1:2].strip()
            if not ic_char.isdigit():
                continue
            ic = int(ic_char)

            # Claude 猜錯文檔結構了，待修
            # if ic not in (1, 2):
            #     continue

            # horl = "H" if ic == 1 else "L"
            if ic not in (0, 1):
                continue

            # IC 代表這一行「第一組事件」的類別，非固定不變
            # IC=0 → 第一組為高潮(H)，之後逐組交替 H/L/H/L...
            # IC=1 → 第一組為低潮(L)，之後逐組交替 L/H/L/H...
            first_horl = "H" if ic == 0 else "L"

            kstn  = line[2:8].strip()
            idy   = int(line[8:10])
            mo    = int(line[10:13])
            iyr2  = int(line[13:15])
            year  = (2000 + iyr2) if iyr2 < _CENTURY_THRESHOLD else (1900 + iyr2)

            # 解析最多 6 組高低潮
            pos = 15
            event_index = 0   # 用來交替判斷 H/L
            while pos + 11 <= len(line):
                time_str = line[pos:pos+5]
                hgt_str  = line[pos+5:pos+11]
                pos += 11

                # 填充欄判斷
                if time_str.strip() == "9999":
                    break

                # 決定這一組事件的 H/L（先算好，不受後面 continue 影響）
                if event_index % 2 == 0:
                    horl = first_horl
                else:
                    horl = "L" if first_horl == "H" else "H"
                event_index += 1

                # 解析時間（HHMM，4 碼，前可能有空格）
                t = time_str.strip()
                if len(t) < 4:
                    continue
                try:
                    hh = int(t[:-2])   # 前1~2位為小時
                    mm = int(t[-2:])   # 後2位為分鐘
                except ValueError:
                    log.warning(f"無法解析時間欄位：'{time_str}'，跳過")
                    continue

                # 解析水位（公尺 → 毫米整數）
                try:
                    hgt_m = float(hgt_str)
                    hgt_mm = round(hgt_m * 1000)
                except ValueError:
                    log.warning(f"無法解析水位欄位：'{hgt_str}'，跳過")
                    continue

                # 組裝 DATATIME
                try:
                    datatime = datetime(year, mo, idy, hh, mm)
                except ValueError as e:
                    log.warning(f"無效日期時間 {year}-{mo}-{idy} {hh}:{mm}：{e}，跳過")
                    continue

                records.append({
                    "STID":           stid,
                    "STID_NEW":       stid_new,
                    "DATATIME":       datatime,
                    "YEAR":           year,
                    "MONTH":          mo,
                    "DAY":            idy,
                    "HOUR":           hh,
                    "MINUTE":         mm,
                    "HORL":           horl,
                    "HEIGHT":         hgt_mm,
                    "QC":             _qc,
                    "sn":             None,
                })

            # 以下程式碼不夠完整，會報錯
            # pos = 15
            # event_index = 0   # 新增：用來交替 H/L
            # while pos + 11 <= len(line):
            #     time_str = line[pos:pos+5]
            #     hgt_str  = line[pos+5:pos+11]
            #     pos += 11

            #     # 填充欄判斷
            #     if time_str.strip() == "9999":
            #         break

            #     # 解析時間（HHMM，4 碼，前可能有空格）
            #     t = time_str.strip()
            #     if len(t) < 4:
            #         continue
            #     try:
            #         hh = int(t[:-2])   # 前1~2位為小時
            #         mm = int(t[-2:])   # 後2位為分鐘
            #     except ValueError:
            #         log.warning(f"無法解析時間欄位：'{time_str}'，跳過")
            #         continue

            #     # 解析水位（公尺 → 毫米整數）
            #     try:
            #         hgt_m = float(hgt_str)
            #         hgt_mm = round(hgt_m * 1000)
            #     except ValueError:
            #         log.warning(f"無法解析水位欄位：'{hgt_str}'，跳過")
            #         continue

            #     # 新增：依 event_index 交替決定這一組的 H/L
            #     if event_index % 2 == 0:
            #         horl = first_horl
            #     else:
            #         horl = "L" if first_horl == "H" else "H"
            #     event_index += 1

            #     records.append({
            #         # ...
            #         "HORL": horl,
            #         # ...
            #     })

            #     # 組裝 DATATIME
            #     try:
            #         datatime = datetime(year, mo, idy, hh, mm)
            #     except ValueError as e:
            #         log.warning(f"無效日期時間 {year}-{mo}-{idy} {hh}:{mm}：{e}，跳過")
            #         continue

            #     records.append({
            #         "STID":           stid,
            #         "STID_NEW":       stid_new,
            #         "DATATIME":       datatime,
            #         "YEAR":           year,
            #         "MONTH":          mo,
            #         "DAY":            idy,
            #         "HOUR":           hh,
            #         "MINUTE":         mm,
            #         "HORL":           horl,
            #         "HEIGHT":         hgt_mm,
            #         "QC":             FORECAST_QC,
            #         "sn":             None,
            #     })

    log.info(f"[解析完成] sta1-prd2-gmt.dat → {len(records)} 筆高低潮記錄")
    return records


# ─────────────────────────────────────────────
#  解析 sta1-gmt.har（分潮調和常數）
# ─────────────────────────────────────────────

def parse_har(har_path: Path) -> list[dict]:
    """
    解析 sta1-gmt.har，回傳分潮常數 list of dict。

    .har 檔含兩個完全相同的塊，只解析第一塊。
    停止條件：當前 NO < prev_no（NO 欄位從 70 重置回 1，偵測到第二塊起點）。

    欄位對映（split() 給 9 或 10 tokens）：
      [0]  NO（整數，跳過）
      [1]  NAME → tidesub.NAME
      [2]  FREQUENCY（cycle/hr）→ tidesub.FREQUENCY
      [3]  STN（跳過）
      中間 M-Y/M-Y（跳過）：月份為個位數時會拆成兩個 token，
           例如 '124/1225' 為 1 個、'625/ 526' 為 2 個，故以下四欄從尾端取
      [-4] A（公尺，未節點修正）→ tidesub.AMPLITUDE
      [-3] G（度，未節點修正）→ tidesub.PHASE
      [-2] AL（公尺，節點修正後）→ tidesub.APLITUDE（原始欄位名含打字錯誤，維持一致）
      [-1] GL（度，節點修正後）→ tidesub.PHASEL

    A/G = 平衡常數（UTide 比較基礎）；AL/GL = 實際預報使用值。
    """
    if not har_path.exists():
        raise FileNotFoundError(f"找不到 .har 檔：{har_path}")

    records: list[dict] = []
    prev_no = 0

    with har_path.open("r", encoding="ascii", errors="replace") as f:
        for line in f:
            parts = line.split()
            # 有效資料行需至少 9 個 token；表頭/空行自然不符
            if len(parts) < 9:
                continue
            try:
                no = int(parts[0])
            except ValueError:
                continue   # parts[0] 非整數 → 表頭行（'NO', 'ANALYSIS' 等）
            # 第二塊起點偵測：NO 從大值重置回 1
            if no < prev_no:
                break
            prev_no = no
            try:
                records.append({
                    "NAME":      parts[1],
                    "FREQUENCY": float(parts[2]),
                    "AMPLITUDE": float(parts[-4]),
                    "PHASE":     float(parts[-3]),
                    "APLITUDE":  float(parts[-2]),
                    "PHASEL":    float(parts[-1]),
                })
            except (ValueError, IndexError) as e:
                log.warning(f"parse_har 跳過行（格式異常）：{e}  →  {line.rstrip()}")

    log.info(f"[解析完成] sta1-gmt.har → {len(records)} 個分潮")
    return records


# ─────────────────────────────────────────────
#  工作目錄準備
# ─────────────────────────────────────────────

def prepare_work_dir(work_dir: Path):
    """建立工作目錄（若不存在）並清除上一次執行殘留的輸出檔。"""
    work_dir.mkdir(parents=True, exist_ok=True)

    # 每次執行前清除舊輸出，避免誤用上次結果
    stale_outputs = [
        "tide2.dat",
        "sta1-gmt.har",
        "tide5.dat",
        "sta1-prd1-gmt.dat",
        "sta1-prd2-gmt.dat",
        "trans1b.log",
        "tide1_r2kb.log",
        "file10.dat",
    ]
    for fname in stale_outputs:
        fpath = work_dir / fname
        if fpath.exists():
            fpath.unlink()
            log.debug(f"已刪除舊檔：{fpath}")

    log.info(f"工作目錄就緒：{work_dir}")


# ─────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────

def main():
    args = parse_args()

    # ── 覆寫工作目錄 ──
    global WORK_DIR, FORTRAN_BIN_DIR
    if args.work_dir:
        WORK_DIR = Path(args.work_dir)
        if not os.environ.get("TIDE_BIN_DIR"):
            FORTRAN_BIN_DIR = WORK_DIR

    # ── Fortran 執行方式 ──
    global RUNNER
    RUNNER = args.runner
    if RUNNER == "docker" and args.work_dir:
        log.warning("docker 模式下容器固定使用掛載的 /work，--work-dir 需與掛載目錄一致")
    if RUNNER == "native":
        log.info(f"native 模式：gfortran = {GFORTRAN}  執行檔目錄 = {FORTRAN_BIN_DIR}")

    # ── 初始化 ENV ──
    try:
        select_env(args.env)
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)
    log.info(f"執行環境：{args.env}  DB={ENV['db']['host']}/{ENV['db']['database']}")

    if args.list_tidesub:
        conn = get_conn()
        try:
            print(list_tidesub(conn, args.stid))
        finally:
            conn.close()
        return

    # ── 解析 flag / pred 日期 ──
    flag_qc = args.flag

    pred_start_date: date | None = None
    pred_end_date:   date | None = None
    try:
        if args.predbeg:
            pred_start_date = date.fromisoformat(args.predbeg)
        if args.predend:
            pred_end_date = date.fromisoformat(args.predend)
    except ValueError as e:
        log.error(f"--predbeg / --predend 日期格式錯誤：{e}")
        sys.exit(1)

    # ── 解析分析日期（flag c 可省略）──
    start_date: date | None = None
    end_date:   date | None = None
    if (args.start is None) != (args.end is None):
        log.error("--start 與 --end 需同時提供或同時省略")
        sys.exit(1)
    try:
        if args.start:
            start_date = date.fromisoformat(args.start)
            end_date   = date.fromisoformat(args.end)
    except ValueError as e:
        log.error(f"日期格式錯誤：{e}")
        sys.exit(1)

    if start_date is not None and start_date > end_date:
        log.error("起始日期不得晚於結束日期")
        sys.exit(1)

    if (pred_start_date is None) != (pred_end_date is None):
        log.error("--predbeg 與 --predend 需同時提供或同時省略")
        sys.exit(1)

    # 預報期間 = --predbeg ~ --predend，同時決定 sta1-bnd1.txt（Fortran 實際預報期間，
    # trans2 會再前後各延一天）以及 tideh / tide6ha / tide1ha 的寫入範圍，四者一致。
    # 未提供時不做預報：只跑分析並寫 tidesub（predbeg/predend 寫 NULL）。
    do_predict = pred_start_date is not None
    prd_filter_start, prd_filter_end = pred_start_date, pred_end_date
    if do_predict and prd_filter_start > prd_filter_end:
        log.error("預報起始日期不得晚於結束日期")
        sys.exit(1)

    # 常數來源：flag c 或 --predict-only 讀 tidesub；其餘由觀測資料重新分析
    from_db = flag_qc == "c" or args.predict_only
    if from_db and not do_predict:
        log.error("flag c 或 --predict-only 只做預報，需提供 --predbeg/--predend")
        sys.exit(1)
    if flag_qc != "c" and start_date is None:
        log.error("flag a/h 需提供 --start/--end（分析期間，或 --predict-only 時沿用哪次分析的常數）")
        sys.exit(1)
    if args.supersede and from_db:
        log.error("--supersede 只用於分析模式（重新分析後取代既有常數）")
        sys.exit(1)

    mode = "只預報（tidesub 常數）" if from_db else ("分析+預報" if do_predict else "只分析")
    log.info(
        f"=== 潮汐預報流程開始 ==="
        f"  站碼={args.stid}  flag={flag_qc}  模式={mode}"
        f"  分析={start_date}~{end_date}"
        f"  pred={f'{prd_filter_start}~{prd_filter_end}' if do_predict else '（不預報）'}"
    )

    # ── 工作目錄準備 ──
    prepare_work_dir(WORK_DIR)

    log.info("連線 MySQL...")
    conn = get_conn()

    try:
        # ── 取得站點元資料 ──
        meta = fetch_station_meta(conn, args.stid)
        stid_new = args.stid_new or meta.get("stid_new")
        lat_deg, lat_min, lon_deg, lon_min = parse_lat_lon_from_meta(meta)
        log.info(
            f"站點資訊：{args.stid}（{meta.get('stnac','')}）"
            f"  緯度={lat_deg}°{lat_min}'  經度={lon_deg}°{lon_min}'"
        )

        # ─────────────────────────────────────────
        #  靜態檔案複製
        # ─────────────────────────────────────────
        copy_static_files(WORK_DIR)

        # ─────────────────────────────────────────
        #  sta1-bnd1.txt（預報期間，僅 trans2 讀取）
        # ─────────────────────────────────────────
        if do_predict:
            build_sta1_bnd1(WORK_DIR, prd_filter_start, prd_filter_end)

        # ─────────────────────────────────────────
        #  分潮常數：讀 tidesub，或由觀測資料分析（步驟 1, 2）
        # ─────────────────────────────────────────
        if from_db:
            har_records, start_date, end_date = fetch_tidesub_constants(
                conn, args.stid, flag_qc, start_date, end_date
            )
            write_har_from_constants(WORK_DIR, args.stid, har_records,
                                     start_date, end_date)
            # sta1-infs1.txt 平常由 trans1b 產生，只預報時自行組裝（trans2 讀取）
            build_sta1_infs1(
                WORK_DIR, lat_deg, lat_min, lon_deg, lon_min, args.stid
            )
        else:
            # 取得水位資料
            daily_data = fetch_water_levels(conn, args.stid, start_date, end_date)

            # 組裝 sta1-local.txt（trans1b 另會輸出 sta1-infs1.txt）
            build_sta1_local(
                WORK_DIR, args.stid,
                lat_deg, lat_min, lon_deg, lon_min,
                daily_data,
            )

            if not args.dry_run:
                # 步驟 1：trans1b
                run_trans1b(args.no_compile)
                # 步驟 2：TIDE1_R2kb
                run_tide1_r2kb(args.no_compile)
            else:
                log.info("[dry-run] 跳過步驟 1, 2（trans1b, TIDE1_R2kb）")

        if args.dry_run:
            log.info("[dry-run] 跳過步驟 3, 4（trans2, TIDE4R2b）及 DB 寫入")
            return

        # ─────────────────────────────────────────
        #  分析模式：比對 tidesub 同一分析期間的既有常數（預報前先檢查，不同就停）
        # ─────────────────────────────────────────
        supersede: list[tuple] = []
        if not from_db:
            har_records = parse_har(WORK_DIR / "sta1-gmt.har")
            report, conflicts = check_tidesub(conn, args.stid, flag_qc,
                                              start_date, end_date, har_records)
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

        # ─────────────────────────────────────────
        #  步驟 3, 4：預報（有指定預報期間才執行）
        # ─────────────────────────────────────────
        minute_records: list[dict] = []
        extreme_records: list[dict] = []
        if do_predict:
            # 步驟 3：trans2
            run_trans2(args.no_compile)
            # 步驟 4：TIDE4R2b
            run_tide4r2b(args.no_compile)

            # prd1 → tide1ha（flag c）或 tide6ha（flag a/h）
            minute_records = parse_prd1(WORK_DIR / "sta1-prd1-gmt.dat",
                                        prd_filter_start, prd_filter_end)
            if not minute_records:
                log.warning("parse_prd1 結果為空，請確認 Fortran 輸出及 pred 日期範圍。")

            # prd2 → tideh（flag a/h）
            if flag_qc != "c":
                records = parse_prd2(WORK_DIR / "sta1-prd2-gmt.dat", args.stid, stid_new, qc=flag_qc)
                # 先標平潮再篩選，讓預報期間首末筆也能與期間外的相鄰事件比較
                records = apply_slack_tide_marking(records)
                extreme_records = [r for r in records
                                   if prd_filter_start <= r["DATATIME"].date() <= prd_filter_end]
                if not extreme_records:
                    log.warning("parse_prd2 結果為空，請確認 Fortran 輸出是否正常。")
        else:
            log.info("未指定 --predbeg/--predend：跳過步驟 3, 4（不預報）")

        # ─────────────────────────────────────────
        #  寫入 DB（flag a/h：tide6ha + tideh + tidesub 同一交易；flag c：tide1ha）
        #  只預報時以本次預報期間另存一份 tidesub
        # ─────────────────────────────────────────
        if args.skip_db_write:
            log.info("[skip-db-write] 跳過寫入 DB")
        else:
            write_outputs(
                conn,
                stid=args.stid, stid_new=stid_new, flag=flag_qc,
                pred_start=pred_start_date, pred_end=pred_end_date,
                minute_records=minute_records, extreme_records=extreme_records,
                constants=har_records, datebeg=start_date, dateend=end_date,
                supersede=supersede,
            )

        log.info("=== 潮汐預報流程完成 ===")

    except Exception as e:
        log.exception(f"流程失敗：{e}")
        sys.exit(1)
    finally:
        conn.close()

# ─────────────────────────────────────────────
#  入口
# ─────────────────────────────────────────────

if __name__ == "__main__":
    main()
