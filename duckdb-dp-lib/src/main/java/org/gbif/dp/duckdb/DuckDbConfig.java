package org.gbif.dp.duckdb;

public interface DuckDbConfig {
    String dbMemory();
    int dbThreads();
    String dbTempDir();
    String dbMaxTemp();
}
