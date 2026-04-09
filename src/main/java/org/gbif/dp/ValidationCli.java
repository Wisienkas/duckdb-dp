package org.gbif.dp;

import java.nio.file.Path;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.dp.duckdb.DuckDbResourceLoader;
import org.gbif.dp.validation.DataPackageValidator;
import org.gbif.dp.validation.DataTypeViolation;
import org.gbif.dp.validation.DuckDbDataPackageValidator;
import org.gbif.dp.validation.ForeignKeyViolation;
import org.gbif.dp.validation.ValidationOptions;
import org.gbif.dp.validation.ValidationResult;

public class ValidationCli {

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: ValidationCli <path-to-datapackage.json>");
      System.exit(1);
    }

    DataPackageValidator validator =
        new DuckDbDataPackageValidator(new JacksonDataPackageParser(), new DuckDbResourceLoader());
    ValidationResult result = validator.validate(Path.of(args[0]), ValidationOptions.defaults());

    if (result.isValid()) {
      System.out.println("All validations passed.");
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

    System.exit(2);
  }
}
