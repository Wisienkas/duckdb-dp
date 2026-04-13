package org.gbif.dp;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.dp.duckdb.DuckDbResourceLoader;
import org.gbif.dp.validation.DataPackageValidator;
import org.gbif.dp.validation.DataTypeViolation;
import org.gbif.dp.validation.DuckDbDataPackageAnalyzer;
import org.gbif.dp.validation.ForeignKeyViolation;
import org.gbif.dp.validation.ValidationOptions;
import org.gbif.dp.validation.ValidationResult;

public class ValidationCli {

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: ValidationCli <paths-to-datapackage.json>");
      System.exit(1);
    }
    Instant startTimer = Instant.now();

    DataPackageValidator validator =
        new DuckDbDataPackageAnalyzer(new JacksonDataPackageParser(), new DuckDbResourceLoader());
    ValidationResult result = validator.validate(Path.of(args[0]), ValidationOptions.defaults());

    if (result.isValid()) {
      System.out.println("All validations passed.");

      printResult(result, startTimer);
      return;
    }

    for (ForeignKeyViolation v : result.foreignKeyViolations()) {
      System.out.printf(
          "FK violation: %s(%s) -> %s(%s), count=%d%n",
          v.resource(),
          String.join(",", v.fields()),
          v.referenceResource(),
          String.join(",", v.referenceFields()),
          v.violationCount());
      for (var sample : v.sampleRows()) {
        System.out.println("  sample=" + sample);
      }
    }

    for (DataTypeViolation v : result.dataTypeViolations()) {
      System.out.printf(
          "Type violation: %s.%s declared as '%s', count=%d%n",
          v.resource(), v.field(), v.declaredType(), v.violationCount());
      for (String sample : v.sampleValues()) {
        System.out.println("  bad value: " + sample);
      }
    }

    printResult(result, startTimer);
    System.exit(2);
  }

  private static void printResult(ValidationResult result, Instant startTimer) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    String resultAsString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
    System.out.println(resultAsString);
    Duration duration = Duration.between(startTimer, Instant.now());
    System.out.printf("duration: %02d:%02d:%02d %n",
            duration.toHoursPart(),
            duration.toMinutesPart(),
            duration.toSecondsPart());
  }
}
