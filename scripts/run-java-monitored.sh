#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <base_log_dir> <data_dir> <jar_path>"
  exit 1
fi

BASE_LOG_DIR="$1"
DATA_DIR="$2"
JAR_PATH="$3"
DATAPACKAGE_PATH="$DATA_DIR/datapackage.json"

RUN_ID="$(date +%Y-%m-%d_%H-%M-%S)_$$"
LOG_DIR="$BASE_LOG_DIR/$RUN_ID"

mkdir -p "$LOG_DIR"
mkdir -p "$LOG_DIR/duckdb-tmp"
ln -sfn "$LOG_DIR" "$BASE_LOG_DIR/latest"

[[ -r "$JAR_PATH" ]] || {
  echo "Jar not readable: $JAR_PATH"
  exit 1
}
[[ -d "$DATA_DIR" ]] || {
  echo "Data dir not found: $DATA_DIR"
  exit 1
}
[[ -f "$DATAPACKAGE_PATH" ]] || {
  echo "Missing datapackage.json in: $DATA_DIR"
  exit 1
}
[[ -w "$LOG_DIR" ]] || {
  echo "Log dir not writable: $LOG_DIR"
  exit 1
}
[[ -w "$LOG_DIR/duckdb-tmp" ]] || {
  echo "DuckDB tmp dir not writable: $LOG_DIR/duckdb-tmp"
  exit 1
}

JAVA_BIN="${JAVA_BIN:-java}"

XMS="${XMS:-512m}"
XMX="${XMX:-2g}"
MAX_METASPACE="${MAX_METASPACE:-256m}"
MAX_DIRECT_MEMORY="${MAX_DIRECT_MEMORY:-256m}"

DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-1500MB}"
DUCKDB_THREADS="${DUCKDB_THREADS:-2}"
DUCKDB_TEMP_DIR="${DUCKDB_TEMP_DIR:-$LOG_DIR/duckdb-tmp}"
DUCKDB_MAX_TEMP_SIZE="${DUCKDB_MAX_TEMP_SIZE:-20GB}"

PIDSTAT_INTERVAL_SEC="${PIDSTAT_INTERVAL_SEC:-1}"
JSTAT_INTERVAL_MS="${JSTAT_INTERVAL_MS:-1000}"
VMSTAT_INTERVAL_SEC="${VMSTAT_INTERVAL_SEC:-1}"

APP_LOG="$LOG_DIR/app.out.log"
GC_LOG="$LOG_DIR/gc.log"
PIDSTAT_LOG="$LOG_DIR/pidstat.log"
JSTAT_LOG="$LOG_DIR/jstat.log"
VMSTAT_LOG="$LOG_DIR/vmstat.log"
SUMMARY_LOG="$LOG_DIR/summary.txt"
ENV_LOG="$LOG_DIR/env.txt"
PID_FILE="$LOG_DIR/app.pid"

command -v "$JAVA_BIN" >/dev/null
command -v pidstat >/dev/null
command -v jstat >/dev/null
command -v vmstat >/dev/null

JCMD_BIN="$(command -v jcmd || true)"

cat >"$ENV_LOG" <<EOF
started_at=$(date --iso-8601=seconds)
run_id=$RUN_ID
base_log_dir=$BASE_LOG_DIR
log_dir=$LOG_DIR
data_dir=$DATA_DIR
datapackage_path=$DATAPACKAGE_PATH
java_bin=$JAVA_BIN
jar_path=$JAR_PATH
xms=$XMS
xmx=$XMX
max_metaspace=$MAX_METASPACE
max_direct_memory=$MAX_DIRECT_MEMORY
duckdb_memory_limit=$DUCKDB_MEMORY_LIMIT
duckdb_threads=$DUCKDB_THREADS
duckdb_temp_dir=$DUCKDB_TEMP_DIR
duckdb_max_temp_size=$DUCKDB_MAX_TEMP_SIZE
pidstat_interval_sec=$PIDSTAT_INTERVAL_SEC
jstat_interval_ms=$JSTAT_INTERVAL_MS
vmstat_interval_sec=$VMSTAT_INTERVAL_SEC
EOF

echo "Starting app..."
echo "Run ID: $RUN_ID"
echo "Logs: $LOG_DIR"
echo "Datapackage: $DATAPACKAGE_PATH"

export DUCKDB_MEMORY_LIMIT
export DUCKDB_THREADS
export DUCKDB_TEMP_DIR
export DUCKDB_MAX_TEMP_SIZE

"$JAVA_BIN" \
  -Xms"$XMS" \
  -Xmx"$XMX" \
  -XX:MaxMetaspaceSize="$MAX_METASPACE" \
  -XX:MaxDirectMemorySize="$MAX_DIRECT_MEMORY" \
  -Xlog:gc*:file="$GC_LOG":time,uptime,level,tags \
  -jar "$JAR_PATH" \
  "$DATAPACKAGE_PATH" \
  > >(tee -a "$APP_LOG") \
  2> >(tee -a "$APP_LOG" >&2) &

JAVA_PID=$!
echo "$JAVA_PID" >"$PID_FILE"
echo "java_pid=$JAVA_PID" | tee -a "$SUMMARY_LOG"

sleep 1

pidstat -r -u -d -h -p "$JAVA_PID" "$PIDSTAT_INTERVAL_SEC" >"$PIDSTAT_LOG" &
PIDSTAT_PID=$!

jstat -gcutil "$JAVA_PID" "$JSTAT_INTERVAL_MS" >"$JSTAT_LOG" &
JSTAT_PID=$!

vmstat "$VMSTAT_INTERVAL_SEC" >"$VMSTAT_LOG" &
VMSTAT_PID=$!

if [[ -n "$JCMD_BIN" ]] && kill -0 "$JAVA_PID" 2>/dev/null; then
  {
    echo "===== VM.flags ====="
    "$JCMD_BIN" "$JAVA_PID" VM.flags || true
    echo
    echo "===== VM.system_properties ====="
    "$JCMD_BIN" "$JAVA_PID" VM.system_properties || true
    echo
    echo "===== VM.native_memory summary ====="
    "$JCMD_BIN" "$JAVA_PID" VM.native_memory summary || true
  } >"$LOG_DIR/jcmd-startup.txt" 2>&1 || true
fi

set +e
wait "$JAVA_PID"
APP_EXIT=$?
set -e

kill "$PIDSTAT_PID" "$JSTAT_PID" "$VMSTAT_PID" 2>/dev/null || true
wait "$PIDSTAT_PID" "$JSTAT_PID" "$VMSTAT_PID" 2>/dev/null || true

if [[ -n "$JCMD_BIN" ]]; then
  {
    echo "===== VM.native_memory summary ====="
    "$JCMD_BIN" "$JAVA_PID" VM.native_memory summary || true
  } >"$LOG_DIR/jcmd-shutdown.txt" 2>&1 || true
fi

cat >>"$SUMMARY_LOG" <<EOF
finished_at=$(date --iso-8601=seconds)
exit_code=$APP_EXIT
app_log=$APP_LOG
gc_log=$GC_LOG
pidstat_log=$PIDSTAT_LOG
jstat_log=$JSTAT_LOG
vmstat_log=$VMSTAT_LOG
pid_file=$PID_FILE
EOF

exit "$APP_EXIT"

