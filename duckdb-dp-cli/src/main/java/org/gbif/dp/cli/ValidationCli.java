package org.gbif.dp.cli;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.dp.analysis.DataPackageAnalyser;
import org.gbif.dp.analysis.DuckDbDataPackageAnalyser;
import org.gbif.dp.analysis.ValidationOptions;
import org.gbif.dp.analysis.model.DataTypeViolation;
import org.gbif.dp.analysis.model.DatapackageAnalysisResult;
import org.gbif.dp.analysis.model.ForeignKeyViolation;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.dp.duckdb.CustomDuckDbConfig;
import org.gbif.dp.duckdb.DuckDbResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

        DataPackageAnalyser validator = new DuckDbDataPackageAnalyser(
                new JacksonDataPackageParser(),
                new DuckDbResourceLoader());
        ValidationOptions defaultOptions = ValidationOptions.defaults();
        ValidationOptions validationOptions = new ValidationOptions(
                defaultOptions.sampleSize(), defaultOptions.jdbcUrl(), customDuckDbConfig);

        DatapackageAnalysisResult result = validator.analyse(
                Path.of(args[0]), validationOptions, List.of());

        Duration duration = Duration.between(startTimer, Instant.now());

        if (arguments.outputFormat == Config.OutputFormat.JSON) {
            printJson(result, duration);
        } else {
            printText(result, duration);
        }

        if (!result.isValid()) {
            System.exit(2);
        }
    }

    private static void printJson(DatapackageAnalysisResult result, Duration duration) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        // Wrap result + duration together so duration is part of the JSON output
        Map<String, Object> output = new LinkedHashMap<>();
        output.put("result", result);
        output.put("durationSeconds", duration.toSeconds());
        System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(output));
    }

    private static void printText(DatapackageAnalysisResult result, Duration duration) {
        if (result.isValid()) {
            System.out.println("All validations passed.");
        }

        for (ForeignKeyViolation v : result.keyViolations()) {
            System.out.printf("FK violation: %s(%s) -> %s(%s), count=%d%n",
                    v.resource(), String.join(",", v.fields()),
                    v.referenceResource(), String.join(",", v.referenceFields()),
                    v.violationCount());
            for (var sample : v.sampleRows()) {
                System.out.println("  sample=" + sample);
            }
        }

        for (DataTypeViolation v : result.dataTypeViolations()) {
            System.out.printf("Type violation: %s.%s declared as '%s', count=%d%n",
                    v.resource(), v.field(), v.declaredType(), v.violationCount());
            for (String sample : v.sampleValues()) {
                System.out.println("  bad value: " + sample);
            }
        }

        System.out.printf("duration: %02d:%02d:%02d%n",
                duration.toHoursPart(), duration.toMinutesPart(), duration.toSecondsPart());
    }
}
