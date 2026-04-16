package org.gbif.dp;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.dp.analysis.AnalysisFeature;
import org.gbif.dp.descriptor.JacksonDataPackageParser;
import org.gbif.dp.duckdb.CustomDuckDbConfig;
import org.gbif.dp.duckdb.DuckDbResourceLoader;
import org.gbif.dp.analysis.DataPackageAnalyser;
import org.gbif.dp.analysis.model.DataTypeViolation;
import org.gbif.dp.analysis.DuckDbDataPackageAnalyser;
import org.gbif.dp.analysis.model.ForeignKeyViolation;
import org.gbif.dp.analysis.ValidationOptions;
import org.gbif.dp.analysis.model.DatapackageAnalysisResult;
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

        DataPackageAnalyser validator = new DuckDbDataPackageAnalyser(
                new JacksonDataPackageParser(),
                new DuckDbResourceLoader());
        ValidationOptions defaultOptions = ValidationOptions.defaults();
        ValidationOptions validationOptions = new ValidationOptions(defaultOptions.sampleSize(), defaultOptions.jdbcUrl(), customDuckDbConfig);
        List<AnalysisFeature> analysisFeatures = List.of(
        );

        DatapackageAnalysisResult result = validator.analyse(Path.of(args[0]), validationOptions, analysisFeatures);

        if (result.isValid()) {
            System.out.println("All validations passed.");

            printResult(result, startTimer);
            return;
        }

        for (ForeignKeyViolation v : result.keyViolations()) {
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

    private static void printResult(DatapackageAnalysisResult result, Instant startTimer) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        String resultAsString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(result);
        System.out.println(resultAsString);
        Duration duration = Duration.between(startTimer, Instant.now());
        System.out.printf("duration: %02d:%02d:%02d %n", duration.toHoursPart(), duration.toMinutesPart(), duration.toSecondsPart());
    }
}
