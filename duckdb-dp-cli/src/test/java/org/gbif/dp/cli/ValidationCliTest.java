package org.gbif.dp.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ValidationCliTest {

    private PrintStream originalOut;
    private ByteArrayOutputStream capturedOut;

    @BeforeEach
    void captureStdout() {
        originalOut = System.out;
        capturedOut = new ByteArrayOutputStream();
        System.setOut(new PrintStream(capturedOut));
    }

    @AfterEach
    void restoreStdout() {
        System.setOut(originalOut);
    }

    @Test
    void shouldExitZeroAndPrintPassedForValidDataset() throws Exception {
        Path tempDir = setupSmallValidDataset();

        int exitCode = ValidationCli.run(new String[]{ tempDir.resolve("datapackage.json").toString() });

        assertEquals(0, exitCode);
        assertTrue(capturedOut.toString().contains("All validations passed."));
    }

    @Test
    void shouldExitTwoForDataTypeViolations() throws Exception {
        Path tempDir = Files.createTempDirectory("dp-cli-invalid-");
        Files.writeString(tempDir.resolve("data.csv"), "id,score\n1,notanumber\n");
        Files.writeString(tempDir.resolve("datapackage.json"), """
            {
              "name": "invalid",
              "resources": [{ "name": "data", "path": "data.csv",
                "schema": { "fields": [
                  { "name": "id",    "type": "integer" },
                  { "name": "score", "type": "number"  }
                ]}}]
            }
            """);

        int exitCode = ValidationCli.run(new String[]{ tempDir.resolve("datapackage.json").toString() });

        assertEquals(2, exitCode);
        assertTrue(capturedOut.toString().contains("Type violation"));
    }

    @Test
    void shouldExitOneForMissingArgs() throws Exception {
        int exitCode = ValidationCli.run(new String[]{});

        assertEquals(1, exitCode);
    }

    @Test
    void shouldOutputValidJsonForValidDataset() throws Exception {
        Path tempDir = setupSmallValidDataset();

        int exitCode = ValidationCli.run(new String[]{
                tempDir.resolve("datapackage.json").toString(),
                "--output-format", "JSON"
        });

        assertEquals(0, exitCode);
        String output = capturedOut.toString();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(output); // throws if not valid JSON
        assertTrue(json.has("result"));
        assertTrue(json.has("durationSeconds"));
        assertTrue(json.get("result").has("valid"));
        assertTrue(json.get("result").get("valid").asBoolean());
    }

    @Test
    void shouldIncludeViolationsInJsonOutput() throws Exception {
        Path tempDir = Files.createTempDirectory("dp-cli-json-violations-");
        Files.writeString(tempDir.resolve("data.csv"), "id,score\n1,asd\n");
        Files.writeString(tempDir.resolve("datapackage.json"), """
            {
              "name": "invalid",
              "resources": [{ "name": "data", "path": "data.csv",
                "schema": { "fields": [
                  { "name": "id",    "type": "integer" },
                  { "name": "score", "type": "number"  }
                ]}}]
            }
            """);

        int exitCode = ValidationCli.run(new String[]{
                tempDir.resolve("datapackage.json").toString(),
                "--output-format", "JSON"
        });

        assertEquals(2, exitCode);
        JsonNode json = new ObjectMapper().readTree(capturedOut.toString());
        assertFalse(json.get("result").path("valid").asBoolean());
        assertFalse(json.get("result").path("dataTypeViolations").isNull());
    }

    private static Path setupSmallValidDataset() throws IOException {
        Path tempDir = Files.createTempDirectory("dp-cli-valid-");
        Files.writeString(tempDir.resolve("data.csv"), "id,score\n1,3.14\n2,2.71\n");
        Files.writeString(tempDir.resolve("datapackage.json"), """
            {
              "name": "clean",
              "resources": [{ "name": "data", "path": "data.csv",
                "schema": { "fields": [
                  { "name": "id",    "type": "integer" },
                  { "name": "score", "type": "number"  }
                ]}}]
            }
            """);
        return tempDir;
    }
}
