# duckdb-dp

Generic Java library to validate Frictionless Data Package referential integrity and column data types
using DuckDB directly over data files.

## What it does

- Parses `datapackage.json` descriptors.
- Creates DuckDB views over resource files (CSV/TSV/Parquet) without loading data into JVM memory.
- **Foreign key validation** — `NOT EXISTS` checks for each `schema.foreignKeys` rule.
- **Data type validation** — `TRY_CAST` checks for each `schema.fields` entry with a declared type (`integer`, `number`, `boolean`, `date`, `datetime`, `time`, `year`, `object`, `array`).
- Returns aggregated violations and sample rows/values per failed rule.

## Main classes

| Class | Responsibility |
|---|---|
| `o.g.dp.descriptor.JacksonDataPackageParser` | Reads and normalizes the JSON descriptor |
| `o.g.dp.descriptor.FieldDescriptor` | Name + Frictionless type + format for a column |
| `o.g.dp.duckdb.DuckDbResourceLoader` | Binds data files as DuckDB views |
| `o.g.dp.validation.DuckDbDataPackageValidator` | Orchestrates all validations |
| `o.g.dp.validation.DuckDbDataTypeValidator` | Column data type checks via `TRY_CAST` |
| `o.g.dp.validation.ValidationResult` | FK + data type violations |
| `o.g.dp.ValidationCli` | Minimal CLI runner |

## Quick start

```bash
mvn test
mvn -q exec:java -Dexec.mainClass=org.gbif.dp.ValidationCli -Dexec.args="/absolute/path/to/datapackage.json"
```

`ValidationCli` exits with:

- `0` when all checks pass
- `2` when violations are found

## Notes for large datasets

- Validation runs in DuckDB with file-backed scans (`read_csv_auto` / `read_parquet`).
- The library only materializes small violation samples in Java.
- You can tune sample size and JDBC URL with `ValidationOptions`.
