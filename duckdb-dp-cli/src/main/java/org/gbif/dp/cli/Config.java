package org.gbif.dp.cli;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "app", mixinStandardHelpOptions = true)
public class Config implements Runnable {

    @Parameters(index = "0", description = "Input data directory")
    public String dataDir;

    @Option(
            names = "--log-dir",
            description = "Directory for logs",
            defaultValue = "${env:LOG_DIR:-./logs}",
            required = false
    )
    public String logDir;

    @Option(
            names = "--duckdb-memory",
            description = "DuckDB memory limit",
            defaultValue = "${env:DUCKDB_MEMORY_LIMIT:-1500MB}"
    )
    public String duckdbMemory;

    @Option(
            names = "--duckdb-threads",
            description = "DuckDB threads",
            defaultValue = "${env:DUCKDB_THREADS:-2}"
    )
    public int duckdbThreads;

    @Option(
            names = "--duckdb-temp-dir",
            description = "DuckDB temp directory",
            defaultValue = "${env:DUCKDB_TEMP_DIR:-./tmp}"
    )
    public String duckdbTempDir;

    @Option(
            names = "--duckdb-max-temp",
            description = "DuckDB max temp size",
            defaultValue = "${env:DUCKDB_MAX_TEMP_SIZE:-20GB}"
    )
    public String duckdbMaxTemp;

    @Option(names = "--verbose", description = "Enable debug logging")
    boolean verbose;

    @Option(names = "--quiet", description = "Only show errors")
    boolean quiet;

    @Override
    public void run() {
        // Just for now
        System.out.println("dataDir = " + dataDir);
        System.out.println("logDir = " + logDir);
        System.out.println("duckdbMemory = " + duckdbMemory);
        System.out.println("duckdbThreads = " + duckdbThreads);
        System.out.println("duckdbTempDir = " + duckdbTempDir);
        System.out.println("duckdbMaxTemp = " + duckdbMaxTemp);
    }
}
