package org.gbif.dp.duckdb;

public class DefaultDuckDbConfig implements DuckDbConfig {
    public static String DEFAULT_DB_MEMORY = "";
    public static String DEFAULT_DB_TEMP_DIR = "";
    public static String DEFAULT_DB_MAX_TEMP = "";
    public static int DEFAULT_DB_MAX_THREADS = 2;

    @Override
    public String dbMemory() {
        return DEFAULT_DB_MEMORY;
    }

    @Override
    public int dbThreads() {
        return DEFAULT_DB_MAX_THREADS;
    }

    @Override
    public String dbTempDir() {
        return DEFAULT_DB_TEMP_DIR;
    }

    @Override
    public String dbMaxTemp() {
        return DEFAULT_DB_MAX_TEMP;
    }
}
