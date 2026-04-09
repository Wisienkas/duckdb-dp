package org.gbif.dp.duckdb;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

public class DuckDbResourceLoader {

  public void createResourceView(Connection connection, String resourceName, Path resourcePath)
      throws SQLException {
    String sql =
        "CREATE OR REPLACE VIEW "
            + quotedIdentifier(resourceName)
            + " AS SELECT * FROM "
            + tableFunction(resourcePath);
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static String tableFunction(Path path) {
    String normalizedPath = path.toAbsolutePath().toString().replace("'", "''");
    String lower = normalizedPath.toLowerCase(Locale.ROOT);
    if (lower.endsWith(".parquet")) {
      return "read_parquet('" + normalizedPath + "')";
    }
    if (lower.endsWith(".tsv")) {
      return "read_csv_auto('" + normalizedPath + "', delim='\\t', header=true, sample_size=-1)";
    }
    return "read_csv_auto('" + normalizedPath + "', header=true, sample_size=-1)";
  }

  private static String quotedIdentifier(String value) {
    return '"' + value.replace("\"", "\"\"") + '"';
  }
}

