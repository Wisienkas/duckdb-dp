package org.gbif.dp.duckdb;

import picocli.CommandLine;

public interface DuckDbConfig {
    String dbMemory();
    int dbThreads();
    String dbTempDir();
    String dbMaxTemp();
}
