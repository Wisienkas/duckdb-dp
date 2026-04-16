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
}

