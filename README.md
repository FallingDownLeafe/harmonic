# tide_pipeline 離線部署（南區 .26 → 安內 .71）

同一份部署包適用兩台目標機（皆無外網、無 gfortran，系統 Python 都不使用）：

| 主機 | 用途 | 系統 | glibc | 系統 Python |
|---|---|---|---|---|
| 南區 .26 | .71 的異地備援；任何會異動 .71 資料庫的程式先在此部署測試 | CentOS 7 | 2.17 | 2.7.5 |
| 安內 .71 | 正式機 | Rocky Linux 8.6 | 2.28 | 3.6.8 |

**相容性以較舊的 .26（glibc 2.17）為準**：wheel 只選 manylinux2014 / manylinux_2_17，Fortran 在 CentOS 7 容器編譯。
在舊 glibc 上編譯、打包的檔案可以在新 glibc 上執行，反之則不行。
（2026-10-06 第一版用 glibc 2.28 的 wheel，在 .26 上 pip 回報 `No matching distribution found`。）

## 部署包內容

| 目錄 | 內容 | 來源 |
|---|---|---|
| `python/` | CPython 3.11 可攜版（python-build-standalone，解開即用、不需 root，需求 glibc ≥ 2.17） | GitHub astral-sh/python-build-standalone，SHA256 已與官方 SHA256SUMS 核對 |
| `wheels/` | mysql-connector-python 26.7.0（純 Python）、utide 0.4.0、numpy 2.2.6、scipy 1.16.3 | `pip download` 限定 manylinux2014 平台（只下載，不在開發機執行） |
| `bin/` | `trans1b`、`tide1_r2kb`、`trans2`、`tide4r2b` 預編執行檔 | Docker `centos:7` 內以 gfortran 4.8.5 編譯，libgfortran / libquadmath 靜態連結，只依賴 glibc ≥ 2.17 |
| `app/` | `tide_pipeline.py`、`tide_db.py`、`utide_pipeline.py`、`tide_utide.py`、`tide2.hdr`、`tide3.dat`、`tide5.hdr`、`.env.example` | 專案根目錄 |

## 在目標機安裝（.26、.71 相同）

```bash
tar -xzf tide_deploy_YYYYMMDD.tar.gz
cd tide_deploy_YYYYMMDD
sha256sum -c SHA256SUMS          # 確認搬運過程檔案無損
bash install.sh                  # 預設裝到 ~/tide 與 ~/python311
vi ~/tide/.env                   # 填 PROD_DB_*（讀寫帳號）
```

`install.sh` 會把 `.env` 的 `TIDE_RUNNER` 設為 `native`、`TIDE_BIN_DIR` 設為 `bin`，之後執行不需再加 `--runner`。
目標機沒有 gfortran，程式會自動使用 `bin/` 的執行檔（log 會顯示「未安裝 gfortran，使用既有執行檔」）。

若之前用舊版部署包裝到一半（pip 失敗），刪掉舊的解壓目錄、改用新包重跑 `install.sh` 即可；已解開的 `~/python311` 會沿用。

## 測試順序（建議）

```bash
cd ~/tide
PY=~/python311/bin/python3
# 0. 舊版曾寫入 1900-01-01 的環境（.26）先執行 docs/tidesub_sentinel_to_null.sql（.71 經 .104 查無此類資料）
# 1. 現況總覽（唯讀）
$PY tide_pipeline.py --env prod --stid 1516 --list-tidesub
# 2. Fortran：只執行不寫 DB，看 tidesub 檢查報告
$PY tide_pipeline.py --env prod --stid 1516 --start 2024-01-01 --end 2025-12-31 --flag a \
    --predbeg 2026-01-01 --predend 2026-01-31 --skip-db-write
# 3. 寫入；報告顯示與既有常數不同（例如 .71 的 2024–2025 a 與廠商 02-02 那組有末位差異）時會停止，
#    確認後加 --supersede
$PY tide_pipeline.py --env prod --stid 1516 --start 2024-01-01 --end 2025-12-31 --flag a \
    --predbeg 2026-01-01 --predend 2026-01-31
# 4. UTide：同樣先 --skip-db-write；flag c 與正式環境現有 tide1ha 比對
$PY utide_pipeline.py --env prod --stid 1516 --start 2024-01-01 --end 2025-12-31 --flag a \
    --predbeg 2026-01-01 --predend 2026-01-31 --skip-db-write
$PY utide_pipeline.py --env prod --stid 1516 --flag c --predbeg 2026-01-01 --predend 2026-01-31 --skip-db-write
```

結束代碼：0 完成、1 錯誤、2 tidesub 檢查發現既有常數不同而未寫入。結束代碼不會印在畫面上，
執行完立刻用 `echo $?` 查看；排程時由 `cron_tide.sh` 記錄。

## 排程（cron_tide.sh）

`install.sh` 會把 `cron_tide.sh` 裝到 `~/tide/`（需與 `tide_pipeline.py`、`tide_db.py` 同目錄）。檔頭可改
`ENGINE`（正式改用 UTide 後改成 `utide_pipeline.py`）、`ENV_NAME`；`STATIONS` 留空即自動挑站：

- a/h：`st` 中 `kind='6' AND reliable='1'`，且分析期間 qc='Q' 整點資料 ≥ 80% 的站
- c：tidesub 有 ah='c' 常數的站（停測站照樣預報）

條件可在 `.env` 以 `TIDE_STATION_FILTER`（SQL WHERE）、`TIDE_MIN_COVERAGE`（0~1）覆寫；略過的站與原因寫在該次 log。
先看會挑到哪些站：`$PY tide_db.py stations --env prod --flag h --start 2024-01-01 --end 2025-12-31`。
手動跑一次確認，再加進 `crontab -e`：

```bash
~/tide/cron_tide.sh monthly-c; echo $?        # 手動測試
tail ~/tide/logs/cron_summary.log             # 歷次排程一行摘要（各站結束代碼）
```

| 模式 | 分析期間 | 預報期間 | 寫入 |
|---|---|---|---|
| `yearly-h` | 前兩個完整年 | 明年全年 | tidesub、tide6ha、tideh（QC h） |
| `yearly-a` | 前兩個完整年 | 去年全年 | tidesub、tide6ha、tideh（QC a） |
| `monthly-c` | 外部匯入的 c 常數 | 下個月整月 | tide1ha（QC c） |

```cron
# m  h  dom mon dow  command
30 2  15  6   *    $HOME/tide/cron_tide.sh yearly-h  >/dev/null 2>&1
30 3  1   2   *    $HOME/tide/cron_tide.sh yearly-a  >/dev/null 2>&1
30 4  20  *   *    $HOME/tide/cron_tide.sh monthly-c >/dev/null 2>&1
```

- 整批結束代碼：0 全部完成；2 有站被 tidesub 檢查擋下（看 log 中的報告，確認後手動加 `--supersede` 重跑該站）；
  1 有站出錯；3 上一次排程還沒結束。排程本身絕不加 `--supersede`。
- 匯入新的 c 常數時，先刪掉該站舊的 c 常數；同一站有兩組 c 分析期間時 flag c 會報錯（結束代碼 1）。


## 已驗證（2026-10-06，開發機上乾淨的 `centos:7` 與 `rockylinux:8.6` 容器，非 root、無 gfortran）

- 依本文件離線安裝 Python 與套件成功。
- 1516、2024 分析、2025-01 預報：`.har`、prd1、prd2 與開發機 Debian 容器（gfortran 12）的輸出逐位元組相同。
- UTide 預報與開發機（utide 0.3.0）相差 < 1e-11 mm。

## 重新產生部署包（開發機）

```bash
# Python（版本更新時改檔名）
curl -LO https://github.com/astral-sh/python-build-standalone/releases/download/<tag>/cpython-3.11.<x>+<tag>-x86_64-unknown-linux-gnu-install_only.tar.gz
# wheels：只列 glibc 2.17 相容平台，pip 會自動挑最新的相容版本
pip download --only-binary=:all: --implementation cp --python-version 3.11 --abi cp311 \
  --platform manylinux2014_x86_64 --platform manylinux_2_17_x86_64 \
  --platform manylinux_2_5_x86_64 --platform manylinux1_x86_64 \
  -d deploy/wheels mysql-connector-python utide
# Fortran 執行檔（PowerShell；$src = workdir\src，$out = deploy\bin）。CentOS 7 已停止維護，套件庫改指 vault
docker run --rm -v "${src}:/src:ro" -v "${out}:/out" centos:7 bash -c "
  sed -i -e 's|^mirrorlist=|#mirrorlist=|' -e 's|^#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|' /etc/yum.repos.d/CentOS-*.repo &&
  yum install -y -q gcc-gfortran libgfortran-static libquadmath-static &&
  rm -f /usr/lib/gcc/x86_64-redhat-linux/4.8.2/libquadmath.so /usr/lib64/libquadmath.so &&
  cd /out && for p in trans1b:trans1b.for tide1_r2kb:TIDE1_R2kb.for trans2:trans2.for tide4r2b:TIDE4R2b.for; do
    gfortran -O2 -ffixed-form -ffixed-line-length-none -std=legacy -static-libgfortran -static-libgcc \
      -o \${p%%:*} /src/\${p##*:}; done && ldd /out/*"
# 打包
bash deploy/make_bundle.sh
```

刪除 `libquadmath.so` 是為了讓連結器改用靜態的 `libquadmath.a`（gcc 沒有 `-static-libquadmath` 選項），只影響臨時容器。
