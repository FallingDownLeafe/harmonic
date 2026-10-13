#!/bin/bash
# 排程用包裝：依模式算出分析／預報日期，逐站執行，寫 log，回傳整批的結束代碼。
#
#   cron_tide.sh yearly-h    調和預報 h：分析前兩個完整年，預報明年全年
#   cron_tide.sh yearly-a    調和分析 a：分析前兩個完整年，預報（重建）去年全年
#   cron_tide.sh monthly-c   最佳常數 c：預報下個月整月（只寫 tide1ha）
#
# 結束代碼：0 全部完成；2 有站因 tidesub 檢查發現既有常數不同而未寫入（需人工看報告，
# 確認後手動加 --supersede 重跑）；1 有站發生錯誤（優先於 2）；3 上一次排程還在跑。
# 排程絕不加 --supersede：取代既有常數要有人看過報告。
#
# 自動挑選站（STATIONS 留空時，由 tide_db.py 從資料庫決定）：
#   a/h：站點表 st 符合 kind='6' AND reliable='1'，且分析期間 qc='Q' 整點資料 ≥ 80%
#   c  ：tidesub 有 ah='c' 常數的站
# 條件可在 .env 以 TIDE_STATION_FILTER（SQL WHERE）與 TIDE_MIN_COVERAGE（0~1）覆寫。
# 略過的站與原因會寫進該次 log。
#
# 設定直接改下面幾行（不讀 .env，避免帳密中的特殊字元被 shell 解讀）。
set -u

STATIONS=""                              # 留空 = 自動挑選（見下）；或手動列站碼，以空白分隔
ENGINE="tide_pipeline.py"                # 正式改用 UTide 後改為 utide_pipeline.py
ENV_NAME="prod"                          # --env；.26 上 PROD_DB_* 指向 .26 自己的庫

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
PY="${PY:-$HOME/python311/bin/python3}"  # install.sh 安裝的 Python
LOG_DIR="$APP_DIR/logs"
mkdir -p "$LOG_DIR"

MODE="${1:-}"
Y=$(date +%Y)
THIS_MONTH=$(date +%Y-%m-01)
case "$MODE" in
  yearly-h)  FLAG=h; START="$((Y-2))-01-01"; END="$((Y-1))-12-31"
             PREDBEG="$((Y+1))-01-01"; PREDEND="$((Y+1))-12-31" ;;
  yearly-a)  FLAG=a; START="$((Y-2))-01-01"; END="$((Y-1))-12-31"
             PREDBEG="$((Y-1))-01-01"; PREDEND="$((Y-1))-12-31" ;;
  monthly-c) FLAG=c; START=""; END=""
             PREDBEG=$(date -d "$THIS_MONTH +1 month" +%F)
             PREDEND=$(date -d "$THIS_MONTH +2 month -1 day" +%F) ;;
  *) echo "用法：$0 yearly-h | yearly-a | monthly-c [其他參數，如 --skip-db-write]" >&2; exit 64 ;;
esac
shift
EXTRA=("$@")                             # 手動測試時可加 --skip-db-write，原樣傳給每一站

# 在終端機手動執行時顯示進度（cron 執行時不輸出，避免寄信）
say() { if [ -t 1 ]; then echo "$@"; fi; }

LOG="$LOG_DIR/$(date +%Y%m%d_%H%M)_${MODE}.log"

# 同一時間只允許一個排程在跑（flock 屬 util-linux，CentOS 7 / Rocky 8 皆內建）
if command -v flock >/dev/null 2>&1; then
    exec 9>"$LOG_DIR/.lock"
    if ! flock -n 9; then
        echo "$(date '+%F %T') 上一次排程尚未結束，略過 $MODE" >> "$LOG_DIR/cron_summary.log"
        exit 3
    fi
else
    echo "警告：找不到 flock，不做重複執行保護" >> "$LOG"
fi

cd "$APP_DIR" || exit 1
if [ -z "$STATIONS" ]; then
    SEL=(--env "$ENV_NAME" --flag "$FLAG")
    [ -n "$START" ] && SEL+=(--start "$START" --end "$END")
    STATIONS=$("$PY" tide_db.py stations "${SEL[@]}" 2>> "$LOG" | tr -d '\r' | cut -f1)
    if [ $? -ne 0 ] || [ -z "$STATIONS" ]; then
        echo "$(date '+%F %T') $MODE 無法取得站清單（見 $(basename "$LOG")）整批=1" >> "$LOG_DIR/cron_summary.log"
        exit 1
    fi
fi
N=$(echo $STATIONS | wc -w)
say "$MODE：共 $N 站，逐站執行中（詳細輸出在 $LOG）"
case " ${EXTRA[*]-} " in (*" --skip-db-write "*) say "※ 有加 --skip-db-write：只計算，不寫入資料庫" ;; esac
worst=0
summary=""
i=0
for S in $STATIONS; do
    i=$((i+1))
    ARGS=(--env "$ENV_NAME" --stid "$S" --flag "$FLAG" --predbeg "$PREDBEG" --predend "$PREDEND")
    [ -n "$START" ] && ARGS+=(--start "$START" --end "$END")
    ARGS+=(${EXTRA[@]+"${EXTRA[@]}"})
    echo "===== $(date '+%F %T') $ENGINE ${ARGS[*]}" >> "$LOG"
    "$PY" "$ENGINE" "${ARGS[@]}" >> "$LOG" 2>&1
    rc=$?
    echo "===== 站 $S 結束代碼 $rc" >> "$LOG"
    say "  [$i/$N] $S 結束代碼 $rc"
    summary="$summary $S=$rc"
    if [ "$rc" -eq 1 ] || { [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ]; }; then
        worst=1
    elif [ "$rc" -eq 2 ] && [ "$worst" -eq 0 ]; then
        worst=2
    fi
done

# 一行摘要，方便 tail 查看歷次排程結果
echo "$(date '+%F %T') $MODE 分析=${START:-（c 常數）}~${END} 預報=$PREDBEG~$PREDEND 結果:$summary 整批=$worst log=$(basename "$LOG")" \
    >> "$LOG_DIR/cron_summary.log"
say "完成，整批結束代碼 $worst（0 全部完成／1 有站錯誤／2 有站常數不同未寫入）"
exit "$worst"
