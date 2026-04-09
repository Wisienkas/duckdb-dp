package org.gbif.dp.validation;

public record ValidationOptions(int sampleSize, String jdbcUrl) {

  public static ValidationOptions defaults() {
    return new ValidationOptions(20, "jdbc:duckdb:");
  }
}

