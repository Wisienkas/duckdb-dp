package org.gbif.dp;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.dp.duckdb.CustomDuckDbConfig;
import org.gbif.dp.duckdb.DuckDbResourceLoader;
import org.gbif.dp.validation.DataPackageValidator;
import org.gbif.dp.validation.model.DataTypeViolation;
import org.gbif.dp.validation.DuckDbDataPackageAnalyzer;
import org.gbif.dp.validation.model.KeyViolation;
import org.gbif.dp.validation.ValidationOptions;
import org.gbif.dp.validation.model.ValidationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class ValidationCli {

    private static final Logger log = LoggerFactory.getLogger(ValidationCli.class);

    public static void main(String[] args) throws Exception {
        Config arguments = new Config();
        CommandLine commandLine = new CommandLine(arguments);
        commandLine.setCaseInsensitiveEnumValuesAllowed(true);
        int executed = commandLine.execute(args);
        if (executed != 0) {
            commandLine.printVersionHelp(System.err);
            System.exit(1);
        }

        if (arguments.quiet) {
            Logging.setRootLevel(Level.ERROR);
        } else if (arguments.verbose) {
            Logging.setRootLevel(Level.DEBUG);
        } else {
            Logging.setRootLevel(Level.INFO);
        }
        Instant startTimer = Instant.now();

        CustomDuckDbConfig customDuckDbConfig = new CustomDuckDbConfig(
                arguments.duckdbMemory,
                arguments.duckdbThreads,
                arguments.duckdbTempDir,
                arguments.duckdbMaxTemp);

        DataPackageValidator validator = new DuckDbDataPackageAnalyzer(
                new JacksonDataPackageParser(),
                new DuckDbResourceLoader());
        ValidationOptions defaultOptions = ValidationOptions.defaults();
        ValidationOptions validationOptions = new ValidationOptions(defaultOptions.sampleSize(), defaultOptions.jdbcUrl(), customDuckDbConfig);
        ValidationResult result = validator.validate(Path.of(args[0]), validationOptions);

        if (result.isValid()) {
            System.out.println("All validations passed.");

            printResult(result, startTimer);
            return;
        }

        for (KeyViolation v : result.keyViolations()) {
            System.out.printf("FK violation: %s(%s) -> %s(%s), count=%d%n", v.resource(), String.join(",", v.fields()), v.referenceResource(), String.join(",", v.referenceFields()), v.violationCount());
            for (var sample : v.sampleRows()) {
                System.out.println("  sample=" + sample);
            }
        }

        for (DataTypeViolation v : result.dataTypeViolations()) {
            System.out.printf("Type violation: %s.%s declared as '%s', count=%d%n", v.resource(), v.field(), v.declaredType(), v.violationCount());
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
        System.out.printf("duration: %02d:%02d:%02d %n", duration.toHoursPart(), duration.toMinutesPart(), duration.toSecondsPart());
    }
}
