# Performance Benchmarking Pipeline

This folder contains scripts and queries used to benchmark and analyze the performance of the DwC data validation application.

The goal is to make it easy to:

* run the validation JAR against different datasets (CSV / TSV / Parquet)
* collect system and JVM metrics during execution
* convert logs into a structured format
* analyze and compare runs using DuckDB

---

## Overview

The pipeline consists of 3 main steps:

1. **Run the application (JAR) with a dataset**
2. **Convert logs into structured Parquet**
3. **Analyze runs using DuckDB + views**

---

## 1. Running the JAR

Each run produces a log folder with:

* application logs (`app.out.log`)
* JVM logs (`gc.log`, `jstat.log`)
* system metrics (`vmstat.log`, `pidstat.log`)
* metadata (`env.txt`, `summary.txt`, `app.pid`)

### Example

```bash
java -jar your-app.jar \
  --dataDir ../data/dwc-dp-parquet \
  --logDir ../log
```

This will produce a run folder like:

```
log/
  2026-04-15_09-44-13_2481507/
    app.out.log
    vmstat.log
    pidstat.log
    jstat.log
    gc.log
    env.txt
    summary.txt
    ...
```

Each folder represents **one run**.

---

## 2. Python: Convert logs to Parquet

The Python script parses all run folders and converts logs into a **single structured table**.

### Setup

```bash
cd scripts
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run

```bash
python parse_runs_to_parquet.py ../log -o out/all_runs.parquet
```

### Output

```
out/all_runs.parquet
```

This is a **tall normalized dataset** with:

* run_id
* source_type (vmstat, pidstat, gc, app, etc.)
* key/value pairs
* numeric values (where possible)
* raw log lines

This file is the **source of truth for analysis**.

---

## 3. DuckDB: Analysis

### Start DuckDB

```bash
duckdb analysis.db
```

### Load views

```bash
.read views.sql
```

This creates:

* `logs` (base view over parquet)
* `run_context` / `run_case`
* `vmstat`, `pidstat`, `jstat`
* `gc_events`, `app_events`, `app_config`
* `run_comparison_simple` (main comparison view)

---

## 4. Compare runs

### Simple comparison

```sql
SELECT *
FROM run_comparison_simple
ORDER BY case_id, total_duration_seconds;
```

### Key metrics

* **avg_cpu_percent**
* **max_memory_mb**
* **avg_memory_mb**
* **duckdb_temp_storage_max_mb**
* **avg_io_wait_percent**
* **total_duration_seconds**

### Case identifier

Runs are grouped by:

```sql
case_id = env.data_dir
```

This represents the dataset / format (CSV, TSV, Parquet).

---

## 5. Export results

### Parquet

```sql
COPY (
  SELECT *
  FROM run_comparison_simple
) TO 'out/run_comparison.parquet'
(FORMAT PARQUET);
```

### CSV

```sql
COPY (
  SELECT *
  FROM run_comparison_simple
) TO 'out/run_comparison.csv'
(HEADER);
```

---

## Folder Structure

```
scripts/
  README.md
  parse_runs_to_parquet.py
  requirements.txt
  views.sql
  .venv/ (ignored)

log/
  <run_id>/

out/
  all_runs.parquet
  run_comparison.parquet
```

---

## Notes

* The Parquet file is **not stored in Git**
* `.venv/` is ignored
* Views are reproducible via `views.sql`
* DuckDB database (`analysis.db`) can be recreated anytime

---

## Terminology

* **Run**: One execution of the JAR
* **Case**: Dataset used (`data_dir`)
* **Tall table**: Key/value structured dataset for logs
* **Pivot views**: Convert tall logs into analyzable tables
* **Summary view**: Aggregated metrics per run

---


## TL;DR

```bash

# 0. Create Jar file
mvn clean package

# 1. Run app → produces logs
java -jar ../target/duckdb-dp-1.0-SNAPSHOT.jar \
  --dataDir data/dwc-dp-parquet \
  --logDir log

# 2. Convert logs
python parse_runs_to_parquet.py ../log -o out/all_runs.parquet

# 3. Analyze
duckdb analysis.db
.read views.sql

# 4. Query
SELECT * FROM run_comparison_simple;
```
