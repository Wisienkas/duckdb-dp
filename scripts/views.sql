-- DuckDB views for parsed run logs
-- Adjust the parquet path below if needed.
-- Usage:
--   duckdb analysis.db -f views.sql
--
-- If your parquet file lives elsewhere, change:
--   'out/all_runs.parquet'
-- to the correct path.

CREATE OR REPLACE VIEW logs AS
SELECT *
FROM read_parquet('data/all_logs.parquet');

CREATE OR REPLACE VIEW env_kv AS
SELECT
  run_id,
  record_id,
  key,
  value,
  value_num,
  value_type,
  raw
FROM logs
WHERE source_type = 'env';

CREATE OR REPLACE VIEW run_context AS
SELECT
  run_id,
  MAX(CASE WHEN source_type = 'env' AND key = 'data_dir' THEN value END) AS data_dir,
  MAX(CASE WHEN source_type = 'env' AND key = 'started_at' THEN value END) AS started_at,
  MAX(CASE WHEN source_type = 'env' AND key = 'log_dir' THEN value END) AS log_dir,
  MAX(CASE WHEN source_type = 'env' AND key = 'base_log_dir' THEN value END) AS base_log_dir,
  MAX(CASE WHEN source_type = 'summary' AND key = 'finished_at' THEN value END) AS finished_at,
  MAX(CASE WHEN source_type = 'summary' AND key = 'exit_code' THEN value END) AS exit_code,
  MAX(CASE WHEN source_type = 'app_pid' AND key = 'pid' THEN value END) AS pid
FROM logs
GROUP BY run_id;

CREATE OR REPLACE VIEW run_case AS
SELECT
  run_id,
  data_dir AS case_id,
  started_at,
  finished_at,
  exit_code,
  pid
FROM run_context;

CREATE OR REPLACE VIEW vmstat AS
SELECT
  run_id,
  record_id,
  MAX(CASE WHEN key = 'r' THEN value_num END) AS r,
  MAX(CASE WHEN key = 'b' THEN value_num END) AS b,
  MAX(CASE WHEN key = 'swpd' THEN value_num END) AS swpd,
  MAX(CASE WHEN key = 'free' THEN value_num END) AS free,
  MAX(CASE WHEN key = 'buff' THEN value_num END) AS buff,
  MAX(CASE WHEN key = 'cache' THEN value_num END) AS cache,
  MAX(CASE WHEN key = 'si' THEN value_num END) AS si,
  MAX(CASE WHEN key = 'so' THEN value_num END) AS so,
  MAX(CASE WHEN key = 'bi' THEN value_num END) AS bi,
  MAX(CASE WHEN key = 'bo' THEN value_num END) AS bo,
  MAX(CASE WHEN key = 'in' THEN value_num END) AS interrupts,
  MAX(CASE WHEN key = 'cs' THEN value_num END) AS context_switches,
  MAX(CASE WHEN key = 'us' THEN value_num END) AS us,
  MAX(CASE WHEN key = 'sy' THEN value_num END) AS sy,
  MAX(CASE WHEN key = 'id' THEN value_num END) AS id,
  MAX(CASE WHEN key = 'wa' THEN value_num END) AS wa,
  MAX(CASE WHEN key = 'st' THEN value_num END) AS st,
  MAX(CASE WHEN key = 'gu' THEN value_num END) AS gu,
  MAX(CASE WHEN key = 'raw' THEN value END) AS raw
FROM logs
WHERE source_type = 'vmstat'
GROUP BY run_id, record_id;

CREATE OR REPLACE VIEW pidstat AS
SELECT
  run_id,
  record_id,
  MAX(CASE WHEN key = 'Time' THEN value END) AS sample_time,
  MAX(CASE WHEN key = 'UID' THEN value_num END) AS uid,
  MAX(CASE WHEN key = 'PID' THEN value_num END) AS pid,
  MAX(CASE WHEN key = '%usr' THEN value_num END) AS pct_usr,
  MAX(CASE WHEN key = '%system' THEN value_num END) AS pct_system,
  MAX(CASE WHEN key = '%guest' THEN value_num END) AS pct_guest,
  MAX(CASE WHEN key = '%wait' THEN value_num END) AS pct_wait,
  MAX(CASE WHEN key = '%CPU' THEN value_num END) AS pct_cpu,
  MAX(CASE WHEN key = 'CPU' THEN value_num END) AS cpu_core,
  MAX(CASE WHEN key = 'minflt/s' THEN value_num END) AS minflt_s,
  MAX(CASE WHEN key = 'majflt/s' THEN value_num END) AS majflt_s,
  MAX(CASE WHEN key = 'VSZ' THEN value_num END) AS vsz,
  MAX(CASE WHEN key = 'RSS' THEN value_num END) AS rss,
  MAX(CASE WHEN key = '%MEM' THEN value_num END) AS pct_mem,
  MAX(CASE WHEN key = 'kB_rd/s' THEN value_num END) AS kb_rd_s,
  MAX(CASE WHEN key = 'kB_wr/s' THEN value_num END) AS kb_wr_s,
  MAX(CASE WHEN key = 'kB_ccwr/s' THEN value_num END) AS kb_ccwr_s,
  MAX(CASE WHEN key = 'iodelay' THEN value_num END) AS iodelay,
  MAX(CASE WHEN key = 'Command' THEN value END) AS command,
  MAX(CASE WHEN key = 'raw' THEN value END) AS raw
FROM logs
WHERE source_type = 'pidstat'
GROUP BY run_id, record_id;

CREATE OR REPLACE VIEW jstat AS
SELECT
  run_id,
  record_id,
  MAX(CASE WHEN key = 'S0' THEN value_num END) AS s0,
  MAX(CASE WHEN key = 'S1' THEN value_num END) AS s1,
  MAX(CASE WHEN key = 'E' THEN value_num END) AS eden_pct,
  MAX(CASE WHEN key = 'O' THEN value_num END) AS old_pct,
  MAX(CASE WHEN key = 'M' THEN value_num END) AS metaspace_pct,
  MAX(CASE WHEN key = 'CCS' THEN value_num END) AS ccs_pct,
  MAX(CASE WHEN key = 'YGC' THEN value_num END) AS ygc,
  MAX(CASE WHEN key = 'YGCT' THEN value_num END) AS ygct,
  MAX(CASE WHEN key = 'FGC' THEN value_num END) AS fgc,
  MAX(CASE WHEN key = 'FGCT' THEN value_num END) AS fgct,
  MAX(CASE WHEN key = 'CGC' THEN value_num END) AS cgc,
  MAX(CASE WHEN key = 'CGCT' THEN value_num END) AS cgct,
  MAX(CASE WHEN key = 'GCT' THEN value_num END) AS gct,
  MAX(CASE WHEN key = 'raw' THEN value END) AS raw
FROM logs
WHERE source_type = 'jstat'
GROUP BY run_id, record_id;

CREATE OR REPLACE VIEW gc_events AS
SELECT
  run_id,
  record_id,
  MAX(CASE WHEN key = 'timestamp' THEN value END) AS timestamp,
  MAX(CASE WHEN key = 'uptime' THEN value END) AS uptime_raw,
  MAX(CASE WHEN key = 'uptime_seconds' THEN value_num END) AS uptime_seconds,
  MAX(CASE WHEN key = 'level' THEN value END) AS level,
  MAX(CASE WHEN key = 'tags' THEN value END) AS tags,
  MAX(CASE WHEN key = 'message' THEN value END) AS message,
  MAX(CASE WHEN key = 'raw' THEN value END) AS raw
FROM logs
WHERE source_type = 'gc'
GROUP BY run_id, record_id;

CREATE OR REPLACE VIEW app_events AS
SELECT
  run_id,
  record_id,
  MAX(CASE WHEN key = 'time' THEN value END) AS time,
  MAX(CASE WHEN key = 'thread' THEN value END) AS thread,
  MAX(CASE WHEN key = 'level' THEN value END) AS level,
  MAX(CASE WHEN key = 'logger' THEN value END) AS logger,
  MAX(CASE WHEN key = 'message' THEN value END) AS message,
  MAX(CASE WHEN key = 'raw' THEN value END) AS raw
FROM logs
WHERE source_type = 'app_out'
  AND key IN ('raw', 'time', 'thread', 'level', 'logger', 'message')
GROUP BY run_id, record_id
HAVING MAX(CASE WHEN key = 'message' THEN 1 ELSE 0 END) = 1;

CREATE OR REPLACE VIEW app_config AS
SELECT
  run_id,
  MAX(CASE WHEN key = 'dataDir' THEN value END) AS app_data_dir,
  MAX(CASE WHEN key = 'logDir' THEN value END) AS app_log_dir,
  MAX(CASE WHEN key = 'duckdbMemory' THEN value END) AS duckdb_memory,
  MAX(CASE WHEN key = 'duckdbThreads' THEN value_num END) AS duckdb_threads,
  MAX(CASE WHEN key = 'duckdbTempDir' THEN value END) AS duckdb_temp_dir,
  MAX(CASE WHEN key = 'duckdbMaxTemp' THEN value END) AS duckdb_max_temp
FROM logs
WHERE source_type = 'app_out'
  AND key IN (
    'dataDir',
    'logDir',
    'duckdbMemory',
    'duckdbThreads',
    'duckdbTempDir',
    'duckdbMaxTemp'
  )
GROUP BY run_id;

CREATE OR REPLACE VIEW run_metrics_summary AS
WITH pidstat_summary AS (
  SELECT
    run_id,
    AVG(pct_cpu) AS avg_pct_cpu,
    MAX(pct_cpu) AS max_pct_cpu,
    AVG(rss) AS avg_rss,
    MAX(rss) AS max_rss,
    AVG(pct_mem) AS avg_pct_mem,
    MAX(pct_mem) AS max_pct_mem,
    AVG(kb_rd_s) AS avg_kb_rd_s,
    MAX(kb_rd_s) AS max_kb_rd_s,
    AVG(kb_wr_s) AS avg_kb_wr_s,
    MAX(kb_wr_s) AS max_kb_wr_s,
    AVG(iodelay) AS avg_iodelay,
    MAX(iodelay) AS max_iodelay
  FROM pidstat
  GROUP BY run_id
),
vmstat_summary AS (
  SELECT
    run_id,
    AVG(us) AS avg_us,
    AVG(sy) AS avg_sy,
    AVG(id) AS avg_id,
    AVG(wa) AS avg_wa,
    MAX(wa) AS max_wa,
    AVG(bi) AS avg_bi,
    AVG(bo) AS avg_bo
  FROM vmstat
  GROUP BY run_id
),
jstat_summary AS (
  SELECT
    run_id,
    MAX(eden_pct) AS max_eden_pct,
    MAX(old_pct) AS max_old_pct,
    MAX(ygc) AS max_ygc,
    MAX(fgc) AS max_fgc,
    MAX(gct) AS max_gct
  FROM jstat
  GROUP BY run_id
),
gc_summary AS (
  SELECT
    run_id,
    COUNT(*) AS gc_event_count
  FROM gc_events
  GROUP BY run_id
)
SELECT
  rc.run_id,
  rc.case_id,
  rc.started_at,
  rc.finished_at,
  rc.exit_code,
  rc.pid,
  ac.duckdb_memory,
  ac.duckdb_threads,
  ac.duckdb_temp_dir,
  ac.duckdb_max_temp,
  ps.avg_pct_cpu,
  ps.max_pct_cpu,
  ps.avg_rss,
  ps.max_rss,
  ps.avg_pct_mem,
  ps.max_pct_mem,
  ps.avg_kb_rd_s,
  ps.max_kb_rd_s,
  ps.avg_kb_wr_s,
  ps.max_kb_wr_s,
  ps.avg_iodelay,
  ps.max_iodelay,
  vs.avg_us,
  vs.avg_sy,
  vs.avg_id,
  vs.avg_wa,
  vs.max_wa,
  vs.avg_bi,
  vs.avg_bo,
  js.max_eden_pct,
  js.max_old_pct,
  js.max_ygc,
  js.max_fgc,
  js.max_gct,
  gs.gc_event_count
FROM run_case rc
LEFT JOIN app_config ac USING (run_id)
LEFT JOIN pidstat_summary ps USING (run_id)
LEFT JOIN vmstat_summary vs USING (run_id)
LEFT JOIN jstat_summary js USING (run_id)
LEFT JOIN gc_summary gs USING (run_id);

CREATE OR REPLACE VIEW run_metrics_summary_with_duration AS
SELECT
  *,
  (try_strptime(finished_at, '%Y-%m-%dT%H:%M:%S%z')
   - try_strptime(started_at, '%Y-%m-%dT%H:%M:%S%z')) AS duration
FROM run_metrics_summary;

CREATE OR REPLACE VIEW run_comparison_human AS
WITH pidstat_summary AS (
  SELECT
    run_id,
    AVG(pct_cpu) AS avg_process_cpu_percent,
    MAX(pct_cpu) AS peak_process_cpu_percent,
    AVG(rss) AS avg_resident_memory_kb,
    MAX(rss) AS peak_resident_memory_kb,
    AVG(pct_mem) AS avg_process_memory_percent,
    MAX(pct_mem) AS peak_process_memory_percent,
    AVG(kb_rd_s) AS avg_read_kb_per_sec,
    MAX(kb_rd_s) AS peak_read_kb_per_sec,
    AVG(kb_wr_s) AS avg_write_kb_per_sec,
    MAX(kb_wr_s) AS peak_write_kb_per_sec,
    AVG(iodelay) AS avg_io_delay,
    MAX(iodelay) AS peak_io_delay
  FROM pidstat
  GROUP BY run_id
),
vmstat_summary AS (
  SELECT
    run_id,
    AVG(us) AS avg_system_user_cpu_percent,
    AVG(sy) AS avg_system_cpu_percent,
    AVG(id) AS avg_system_idle_percent,
    AVG(wa) AS avg_system_io_wait_percent,
    MAX(wa) AS peak_system_io_wait_percent,
    AVG(bi) AS avg_blocks_received_per_sec,
    AVG(bo) AS avg_blocks_sent_per_sec,
    AVG(free) AS avg_free_memory_kb,
    AVG(cache) AS avg_cache_memory_kb
  FROM vmstat
  GROUP BY run_id
),
jstat_summary AS (
  SELECT
    run_id,
    MAX(eden_pct) AS peak_eden_space_percent,
    MAX(old_pct) AS peak_old_generation_percent,
    MAX(metaspace_pct) AS peak_metaspace_percent,
    MAX(ccs_pct) AS peak_compressed_class_space_percent,
    MAX(ygc) AS peak_young_gc_count,
    MAX(fgc) AS peak_full_gc_count,
    MAX(cgc) AS peak_concurrent_gc_count,
    MAX(gct) AS peak_total_gc_time_seconds
  FROM jstat
  GROUP BY run_id
),
gc_summary AS (
  SELECT
    run_id,
    COUNT(*) AS gc_event_count
  FROM gc_events
  GROUP BY run_id
),
base AS (
  SELECT
    rc.run_id,
    rc.case_id,
    rc.started_at,
    rc.finished_at,
    try_strptime(rc.started_at, '%Y-%m-%dT%H:%M:%S%z') AS started_at_ts,
    try_strptime(rc.finished_at, '%Y-%m-%dT%H:%M:%S%z') AS finished_at_ts,
    rc.exit_code,
    rc.pid,
    ac.app_data_dir AS app_data_dir,
    ac.duckdb_memory AS duckdb_memory_limit,
    ac.duckdb_threads AS duckdb_thread_count,
    ac.duckdb_temp_dir AS duckdb_temp_directory,
    ac.duckdb_max_temp AS duckdb_max_temp_space
  FROM run_case rc
  LEFT JOIN app_config ac USING (run_id)
)
SELECT
  b.run_id AS run_id,
  b.case_id AS case_id,
  b.app_data_dir AS application_data_directory,
  b.started_at_ts AS started_at,
  b.finished_at_ts AS finished_at,
  (b.finished_at_ts - b.started_at_ts) AS run_duration,
  epoch_ms(b.finished_at_ts) - epoch_ms(b.started_at_ts) AS run_duration_milliseconds,
  b.exit_code AS exit_code,
  b.pid AS java_process_id,

  b.duckdb_memory_limit AS duckdb_memory_limit,
  b.duckdb_thread_count AS duckdb_thread_count,
  b.duckdb_temp_directory AS duckdb_temp_directory,
  b.duckdb_max_temp_space AS duckdb_max_temp_space,

  ps.avg_process_cpu_percent AS avg_process_cpu_percent,
  ps.peak_process_cpu_percent AS peak_process_cpu_percent,
  ps.avg_resident_memory_kb AS avg_resident_memory_kb,
  ps.peak_resident_memory_kb AS peak_resident_memory_kb,
  ps.avg_process_memory_percent AS avg_process_memory_percent,
  ps.peak_process_memory_percent AS peak_process_memory_percent,
  ps.avg_read_kb_per_sec AS avg_read_kb_per_sec,
  ps.peak_read_kb_per_sec AS peak_read_kb_per_sec,
  ps.avg_write_kb_per_sec AS avg_write_kb_per_sec,
  ps.peak_write_kb_per_sec AS peak_write_kb_per_sec,
  ps.avg_io_delay AS avg_io_delay,
  ps.peak_io_delay AS peak_io_delay,

  vs.avg_system_user_cpu_percent AS avg_system_user_cpu_percent,
  vs.avg_system_cpu_percent AS avg_system_cpu_percent,
  vs.avg_system_idle_percent AS avg_system_idle_percent,
  vs.avg_system_io_wait_percent AS avg_system_io_wait_percent,
  vs.peak_system_io_wait_percent AS peak_system_io_wait_percent,
  vs.avg_blocks_received_per_sec AS avg_blocks_received_per_sec,
  vs.avg_blocks_sent_per_sec AS avg_blocks_sent_per_sec,
  vs.avg_free_memory_kb AS avg_free_memory_kb,
  vs.avg_cache_memory_kb AS avg_cache_memory_kb,

  js.peak_eden_space_percent AS peak_eden_space_percent,
  js.peak_old_generation_percent AS peak_old_generation_percent,
  js.peak_metaspace_percent AS peak_metaspace_percent,
  js.peak_compressed_class_space_percent AS peak_compressed_class_space_percent,
  js.peak_young_gc_count AS peak_young_gc_count,
  js.peak_full_gc_count AS peak_full_gc_count,
  js.peak_concurrent_gc_count AS peak_concurrent_gc_count,
  js.peak_total_gc_time_seconds AS peak_total_gc_time_seconds,

  gs.gc_event_count AS gc_event_count
FROM base b
LEFT JOIN pidstat_summary ps USING (run_id)
LEFT JOIN vmstat_summary vs USING (run_id)
LEFT JOIN jstat_summary js USING (run_id)
LEFT JOIN gc_summary gs USING (run_id);

CREATE OR REPLACE VIEW run_comparison_simple AS
WITH pidstat_summary AS (
  SELECT
    run_id,
    AVG(pct_cpu) AS avg_cpu_percent,
    MAX(rss) / 1024.0 AS max_memory_mb,
    AVG(rss) / 1024.0 AS avg_memory_mb
  FROM pidstat
  GROUP BY run_id
),
vmstat_summary AS (
  SELECT
    run_id,
    AVG(wa) AS avg_io_wait_percent,
    MAX(wa) AS peak_io_wait_percent
  FROM vmstat
  GROUP BY run_id
),
base AS (
  SELECT
    rc.run_id,
    rc.case_id,
    try_strptime(rc.started_at, '%Y-%m-%dT%H:%M:%S%z') AS started_at_ts,
    try_strptime(rc.finished_at, '%Y-%m-%dT%H:%M:%S%z') AS finished_at_ts,
    ac.duckdb_max_temp
  FROM run_case rc
  LEFT JOIN app_config ac USING (run_id)
)
SELECT
  b.case_id,
  b.run_id,
  (b.finished_at_ts - b.started_at_ts) AS total_duration,
  ROUND((epoch_ms(b.finished_at_ts) - epoch_ms(b.started_at_ts)) / 1000.0, 3) AS total_duration_seconds,

  ROUND(ps.avg_cpu_percent, 2) AS avg_cpu_percent,

  ROUND(ps.max_memory_mb, 2) AS max_memory_mb,
  ROUND(ps.avg_memory_mb, 2) AS avg_memory_mb,

  CASE
    WHEN b.duckdb_max_temp IS NULL THEN NULL
    WHEN b.duckdb_max_temp LIKE '%GB' THEN CAST(REPLACE(b.duckdb_max_temp, 'GB', '') AS DOUBLE) * 1024
    WHEN b.duckdb_max_temp LIKE '%MB' THEN CAST(REPLACE(b.duckdb_max_temp, 'MB', '') AS DOUBLE)
    WHEN b.duckdb_max_temp LIKE '%KB' THEN CAST(REPLACE(b.duckdb_max_temp, 'KB', '') AS DOUBLE) / 1024
    ELSE NULL
  END AS duckdb_temp_storage_max_mb,

  ROUND(vs.avg_io_wait_percent, 2) AS avg_io_wait_percent,
  ROUND(vs.peak_io_wait_percent, 2) AS peak_io_wait_percent

FROM base b
LEFT JOIN pidstat_summary ps USING (run_id)
LEFT JOIN vmstat_summary vs USING (run_id);
