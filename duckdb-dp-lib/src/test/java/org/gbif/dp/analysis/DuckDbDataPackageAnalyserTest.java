package org.gbif.dp.analysis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.gbif.dp.analysis.model.ColumnStatistics;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.dp.duckdb.DuckDbResourceLoader;
import org.gbif.dp.analysis.model.DataTypeViolation;
import org.gbif.dp.analysis.model.DatapackageAnalysisResult;
import org.junit.jupiter.api.Test;

class DuckDbDataPackageAnalyserTest {

  @Test
  void shouldValidateForeignKeysFromDescriptor() throws Exception {
    Path tempDir = Files.createTempDirectory("dp-test-");

    Files.writeString(
        tempDir.resolve("parent.csv"),
        "id,name\n1,earth\n2,mars\n");
    Files.writeString(
        tempDir.resolve("child.csv"),
        "id,parent_id\n10,1\n11,999\n12,\n");
    Files.writeString(
        tempDir.resolve("datapackage.json"),
        """
        {
          "name": "sample",
          "resources": [
            {
              "name": "parent",
              "path": "parent.csv"
            },
            {
              "name": "child",
              "path": "child.csv",
              "schema": {
                "foreignKeys": [
                  {
                    "fields": "parent_id",
                    "reference": {
                      "resource": "parent",
                      "fields": "id"
                    }
                  }
                ]
              }
            }
          ]
        }
        """);

    DataPackageAnalyser validator =
        new DuckDbDataPackageAnalyser(new JacksonDataPackageParser(), new DuckDbResourceLoader());

    DatapackageAnalysisResult result = validator.analyse(
            tempDir.resolve("datapackage.json"),
            ValidationOptions.defaults(),
            AnalysisFeature.ALL_FEATURES);

    assertFalse(result.isValid());
    assertEquals(1, result.keyViolations().size());
    assertEquals(1, result.keyViolations().get(0).violationCount());
  }

  @Test
  void shouldDetectDataTypeViolations() throws Exception {
    Path tempDir = Files.createTempDirectory("dp-dtype-test-");

    Files.writeString(
        tempDir.resolve("data.csv"),
        "id,age,active,birth_date\n1,25,true,2000-01-01\n2,notanumber,false,2001-06-15\n3,30,maybe,not-a-date\n");
    Files.writeString(
        tempDir.resolve("datapackage.json"),
        """
        {
          "name": "typed",
          "resources": [
            {
              "name": "data",
              "path": "data.csv",
              "schema": {
                "fields": [
                  { "name": "id",         "type": "integer" },
                  { "name": "age",        "type": "integer" },
                  { "name": "active",     "type": "boolean" },
                  { "name": "birth_date", "type": "date" }
                ]
              }
            }
          ]
        }
        """);

    DataPackageAnalyser validator =
        new DuckDbDataPackageAnalyser(new JacksonDataPackageParser(), new DuckDbResourceLoader());

    DatapackageAnalysisResult result = validator.analyse(
            tempDir.resolve("datapackage.json"),
            ValidationOptions.defaults(),
            AnalysisFeature.ALL_FEATURES);

    assertTrue(result.keyViolations().isEmpty());
    assertFalse(result.isValid());
    // age has 1 bad value ("notanumber"), active has 1 ("maybe"), birth_date has 1 ("not-a-date")
    assertEquals(3, result.dataTypeViolations().size());

    DataTypeViolation ageViolation = result.dataTypeViolations().stream()
        .filter(v -> v.field().equals("age")).findFirst().orElseThrow();
    assertEquals(1, ageViolation.violationCount());
    assertEquals("integer", ageViolation.declaredType());
    assertTrue(ageViolation.sampleValues().contains("notanumber"));

    DataTypeViolation activeViolation = result.dataTypeViolations().stream()
        .filter(v -> v.field().equals("active")).findFirst().orElseThrow();
    assertEquals(1, activeViolation.violationCount());

    DataTypeViolation dateViolation = result.dataTypeViolations().stream()
        .filter(v -> v.field().equals("birth_date")).findFirst().orElseThrow();
    assertEquals(1, dateViolation.violationCount());
    assertTrue(dateViolation.sampleValues().contains("not-a-date"));
  }

  @Test
  void shouldPassWhenAllTypesAreCorrect() throws Exception {
    Path tempDir = setupSmallValidDataset();

    DataPackageAnalyser validator =
        new DuckDbDataPackageAnalyser(new JacksonDataPackageParser(), new DuckDbResourceLoader());

    DatapackageAnalysisResult result = validator.analyse(
            tempDir.resolve("datapackage.json"),
            ValidationOptions.defaults(),
            AnalysisFeature.ALL_FEATURES);

    assertTrue(result.isValid());
  }

  @Test
  void shouldHaveJustTwoLines() throws Exception {
    Path tempDir = setupSmallValidDataset();

    DataPackageAnalyser validator =
            new DuckDbDataPackageAnalyser(new JacksonDataPackageParser(), new DuckDbResourceLoader());

    DatapackageAnalysisResult result = validator.analyse(
            tempDir.resolve("datapackage.json"),
            ValidationOptions.defaults(),
            List.of(AnalysisFeature.COUNT, AnalysisFeature.COUNT_DISTINCT));

    ResourceAnalysisResult resourceAnalysisResult = result.resourceAnalysisResults().stream()
            .filter(rar -> rar.name().equalsIgnoreCase("data"))
            .findFirst().orElseThrow(() -> new AssertionError("Unable to find resourceAnalysisResult for Resource[data]"));

    ColumnStatistics columnStatistics = resourceAnalysisResult.columnAnalyses().stream()
            .filter(c -> c.name().equalsIgnoreCase("score"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Unable to find [data]['score'] column"));

    assertEquals(3, resourceAnalysisResult.totalRows());
    assertEquals(3, columnStatistics.populatedValues());
    assertEquals(2, columnStatistics.uniqueValues());

  }
  @Test
  void shouldOnlyCalculateChoosenFeatures() throws Exception {
    Path tempDir = setupSmallValidDataset();

    DataPackageAnalyser validator =
            new DuckDbDataPackageAnalyser(new JacksonDataPackageParser(), new DuckDbResourceLoader());

    DatapackageAnalysisResult result = validator.analyse(
            tempDir.resolve("datapackage.json"),
            ValidationOptions.defaults(),
            List.of(AnalysisFeature.DATA_TYPE_CONSTRAINT));

      assertTrue(
              result.resourceAnalysisResults().stream()
                .allMatch(rar -> rar.columnAnalyses().isEmpty()),
              "No counting etc for any of the data files");
  }

  @Test
  void shouldFailOnDuplicatePrimaryKeys() {

  }

  private static Path setupSmallValidDataset() throws IOException {
    Path tempDir = Files.createTempDirectory("dp-dtype-pass-");

    Files.writeString(
            tempDir.resolve("data.csv"),
            "id,score\n" +
                    "1,3.14\n" +
                    "2,2.71\n" +
                    "3,2.71\n");
    Files.writeString(
            tempDir.resolve("datapackage.json"),
            """
            {
              "name": "clean",
              "resources": [
                {
                  "name": "data",
                  "path": "data.csv",
                  "schema": {
                    "fields": [
                      { "name": "id",    "type": "integer" },
                      { "name": "score", "type": "number" }
                    ]
                  }
                }
              ]
            }
            """);
    return tempDir;
  }

  @Test
  void shouldNotReQuoteAlreadyQouted() {
    String value = "\"field\"";
    String reqouted = DuckDbDataPackageAnalyser.q(value);

    assertEquals(value, reqouted, "Expect multiple repeated invocations of qouting to have no effect");
  }

  @Test
  void shouldNotCountFieldLevelMissingValuesAsPopulated() throws Exception {
    Path tempDir = Files.createTempDirectory("dp-missing-field-");

    Files.writeString(
            tempDir.resolve("data.csv"),
            "id,score\n1,3.14\n-1,2.71\n3,-1\n");
    Files.writeString(
            tempDir.resolve("datapackage.json"),
            """
            {
              "name": "missing-field",
              "resources": [
                {
                  "name": "data",
                  "path": "data.csv",
                  "schema": {
                    "fields": [
                      { "name": "id",    "type": "integer", "missingValues": ["-1"] },
                      { "name": "score", "type": "number",  "missingValues": ["-1"] }
                    ]
                  }
                }
              ]
            }
            """);

    DataPackageAnalyser validator =
            new DuckDbDataPackageAnalyser(new JacksonDataPackageParser(), new DuckDbResourceLoader());

    DatapackageAnalysisResult result = validator.analyse(
            tempDir.resolve("datapackage.json"),
            ValidationOptions.defaults(),
            List.of(AnalysisFeature.COUNT, AnalysisFeature.COUNT_DISTINCT));

    ColumnStatistics idStats = getColumnStats(result, "data", "id");
    ColumnStatistics scoreStats = getColumnStats(result, "data", "score");

    // Row 2 has "NA" for id, row 3 has "-1" for score — both should be treated as null
    assertEquals(2, idStats.populatedValues(),    "id: '-1' is missing, so only rows 1 and 3 count");
    assertEquals(2, scoreStats.populatedValues(), "score: '-1' is missing, so only rows 1 and 2 count");
    assertEquals(2, idStats.uniqueValues(),       "id: 1 and 3");
    assertEquals(2, scoreStats.uniqueValues(),    "score: 3.14 and 2.71");
  }

  @Test
  void shouldFallBackToSchemaMissingValuesWhenFieldDoesNotDefineOwn() throws Exception {
    Path tempDir = Files.createTempDirectory("dp-missing-schema-");

    Files.writeString(
            tempDir.resolve("data.csv"),
            "id,score\n1,3.14\n2,-1\n3,2.71\n");
    Files.writeString(
            tempDir.resolve("datapackage.json"),
            """
            {
              "name": "missing-schema",
              "resources": [
                {
                  "name": "data",
                  "path": "data.csv",
                  "schema": {
                    "missingValues": ["-1"],
                    "fields": [
                      { "name": "id",    "type": "integer" },
                      { "name": "score", "type": "number"  }
                    ]
                  }
                }
              ]
            }
            """);

    DataPackageAnalyser validator =
            new DuckDbDataPackageAnalyser(new JacksonDataPackageParser(), new DuckDbResourceLoader());

    DatapackageAnalysisResult result = validator.analyse(
            tempDir.resolve("datapackage.json"),
            ValidationOptions.defaults(),
            List.of(AnalysisFeature.COUNT, AnalysisFeature.COUNT_DISTINCT));

    ColumnStatistics scoreStats = getColumnStats(result, "data", "score");

    assertEquals(2, scoreStats.populatedValues(), "score: '-1' inherited from schema counts as missing");
    assertEquals(2, scoreStats.uniqueValues());
  }

  @Test
  void shouldPreferFieldMissingValuesOverSchemaMissingValues() throws Exception {
    Path tempDir = Files.createTempDirectory("dp-missing-precedence-");

    // "-999" is declared missing at schema level
    // "-1" is declared missing at field level for score — so "-999" should NOT be missing for score
    Files.writeString(
            tempDir.resolve("data.csv"),
            "id,score\n1,3.14\n2,-1\n3,-999\n");
    Files.writeString(
            tempDir.resolve("datapackage.json"),
            """
            {
              "name": "missing-precedence",
              "resources": [
                {
                  "name": "data",
                  "path": "data.csv",
                  "schema": {
                    "missingValues": ["-999"],
                    "fields": [
                      { "name": "id",    "type": "integer" },
                      { "name": "score", "type": "number", "missingValues": ["-1"] }
                    ]
                  }
                }
              ]
            }
            """);

    DataPackageAnalyser validator =
            new DuckDbDataPackageAnalyser(new JacksonDataPackageParser(), new DuckDbResourceLoader());

    DatapackageAnalysisResult result = validator.analyse(
            tempDir.resolve("datapackage.json"),
            ValidationOptions.defaults(),
            List.of(AnalysisFeature.COUNT, AnalysisFeature.COUNT_DISTINCT));

    ColumnStatistics idStats    = getColumnStats(result, "data", "id");
    ColumnStatistics scoreStats = getColumnStats(result, "data", "score");

    // id has no field-level override → inherits schema → "-999" is missing → rows 1 and 2 populated
    assertEquals(3, idStats.populatedValues(),    "id: '-999' is missing via schema fallback");
    // score has field-level override → only "-1" is missing → "-999" is a real value → rows 1 and 3 populated
    assertEquals(2, scoreStats.populatedValues(), "score: '-1' is missing, but '-999' is a real value");
    assertEquals(2, scoreStats.uniqueValues(),    "score: 3.14 and -999 are the two populated distinct values");
  }

  @Test
  void shouldUseDefaultMissingValueOfEmptyStringWhenNeitherFieldNorSchemaDefinesMissingValues() throws Exception {
    Path tempDir = Files.createTempDirectory("dp-missing-default-");

    // No missingValues anywhere — only truly empty cells count as null
    Files.writeString(
            tempDir.resolve("data.csv"),
            "id,score\n1,3.14\n2,-1\n3,\n");
    Files.writeString(
            tempDir.resolve("datapackage.json"),
            """
            {
              "name": "missing-default",
              "resources": [
                {
                  "name": "data",
                  "path": "data.csv",
                  "schema": {
                    "fields": [
                      { "name": "id",    "type": "integer" },
                      { "name": "score", "type": "number"  }
                    ]
                  }
                }
              ]
            }
            """);

    DataPackageAnalyser validator =
            new DuckDbDataPackageAnalyser(new JacksonDataPackageParser(), new DuckDbResourceLoader());

    DatapackageAnalysisResult result = validator.analyse(
            tempDir.resolve("datapackage.json"),
            ValidationOptions.defaults(),
            List.of(AnalysisFeature.COUNT, AnalysisFeature.COUNT_DISTINCT));

    ColumnStatistics scoreStats = getColumnStats(result, "data", "score");

    // Row 3 is empty → null. Row 2 "-1" is not a declared missing value → populated (but fails type check)
    assertEquals(2, scoreStats.populatedValues(), "score: empty cell is null, '-1' is a real (if invalid) value");
  }

  // Helper to avoid repetition
  private static ColumnStatistics getColumnStats(DatapackageAnalysisResult result, String resource, String column) {
    return result.resourceAnalysisResults().stream()
            .filter(r -> r.name().equalsIgnoreCase(resource))
            .findFirst().orElseThrow(() -> new AssertionError("Resource not found: " + resource))
            .columnAnalyses().stream()
            .filter(c -> c.name().equalsIgnoreCase(column))
            .findFirst().orElseThrow(() -> new AssertionError("Column not found: " + column));
  }
}

