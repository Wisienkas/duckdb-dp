package org.gbif.dp.validation;

import org.gbif.dp.duckdb.DefaultDuckDbConfig;
import org.gbif.dp.duckdb.DuckDbConfig;

public record ValidationOptions(int sampleSize, String jdbcUrl, DuckDbConfig duckDbConfig) {

  public static ValidationOptions defaults() {
    return new ValidationOptions(20, "jdbc:duckdb:", new DefaultDuckDbConfig());
  }
}

