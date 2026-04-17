package org.gbif.dp.duckdb;

public record CustomDuckDbConfig(
        String dbMemory,
        int dbThreads,
        String dbTempDir,
        String dbMaxTemp
) implements DuckDbConfig {
}
