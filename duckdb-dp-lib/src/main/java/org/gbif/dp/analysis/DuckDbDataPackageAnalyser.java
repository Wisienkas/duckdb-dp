package org.gbif.dp.analysis;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.gbif.dp.descriptor.*;
import org.gbif.dp.duckdb.DuckDbResourceLoader;
import org.gbif.dp.analysis.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DuckDbDataPackageAnalyser implements DataPackageAnalyser {

    private static final Logger log = LoggerFactory.getLogger(DuckDbDataPackageAnalyser.class);

    private final DataPackageParser parser;
    private final DuckDbResourceLoader resourceLoader;
    private final DuckDbDataTypeValidator dataTypeValidator;

    public DuckDbDataPackageAnalyser(DataPackageParser parser, DuckDbResourceLoader resourceLoader) {
        this(parser, resourceLoader, new DuckDbDataTypeValidator());
    }

    public DuckDbDataPackageAnalyser(
            DataPackageParser parser,
            DuckDbResourceLoader resourceLoader,
            DuckDbDataTypeValidator dataTypeValidator) {
        this.parser = parser;
        this.resourceLoader = resourceLoader;
        this.dataTypeValidator = dataTypeValidator;
    }

    @Override
    public DatapackageAnalysisResult analyse(Path descriptorPath, ValidationOptions options, List<AnalysisFeature> analysisFeatures)
            throws IOException, SQLException {
        DataPackageDescriptor dataPackageDescriptor = parser.parse(descriptorPath);

        try (Connection connection = DriverManager.getConnection(options.jdbcUrl())) {
            applyDuckDbOptions(options, connection);

            for (ResourceDescriptor resource : dataPackageDescriptor.resources()) {
                log.info("Creating temp table for {} -> {}", resource.name(), resource.paths());
                resourceLoader.createResourceTempTable(connection, resource.name(), resource.paths());
            }

            List<ResourceAnalysisResult> resourceAnalysisResults = analyseEachResource(options, analysisFeatures, dataPackageDescriptor, connection);

            return new DatapackageAnalysisResult(
                    List.copyOf(resourceAnalysisResults)
            );
        }
    }

    private List<DataTypeViolation> analyseDataTypeViolations(
            ValidationOptions options,
            DataPackageDescriptor dataPackageDescriptor,
            Connection connection)
            throws SQLException {
        List<DataTypeViolation> dataTypeViolations = new ArrayList<>();
        // Data type validation
        for (ResourceDescriptor resource : dataPackageDescriptor.resources()) {
            dataTypeViolations.addAll(dataTypeValidator.validate(connection, resource, options.sampleSize()));
        }
        return dataTypeViolations;
    }

    private List<ResourceAnalysisResult> analyseEachResource(ValidationOptions options, List<AnalysisFeature> analysisFeatures, DataPackageDescriptor dataPackageDescriptor, Connection connection) throws SQLException {
        List<ResourceAnalysisResult> resourceAnalysisResults = new ArrayList<>();
        for (ResourceDescriptor resource : dataPackageDescriptor.resources()) {
            ResourceAnalysisResult resourceAnalysisResult = analyseResource(
                    options,
                    resource,
                    connection,
                    dataPackageDescriptor,
                    analysisFeatures);
            resourceAnalysisResults.add(resourceAnalysisResult);
        }
        return resourceAnalysisResults;
    }

    private void applyDuckDbOptions(ValidationOptions options, Connection connection) throws SQLException {
        if (options.duckDbConfig() != null) {
            try (Statement st = connection.createStatement()) {
                if (!options.duckDbConfig().dbMemory().isBlank()) {
                    st.execute("PRAGMA memory_limit = " + sq(options.duckDbConfig().dbMemory()));
                }
                if (options.duckDbConfig().dbThreads() > 0) {
                    st.execute("PRAGMA threads = " + options.duckDbConfig().dbThreads());
                }
                if (!options.duckDbConfig().dbTempDir().isBlank()) {
                    st.execute("PRAGMA temp_directory = " + sq(options.duckDbConfig().dbTempDir()));
                }
                if (!options.duckDbConfig().dbMaxTemp().isBlank())
                    st.execute("PRAGMA memory_limit = " + sq(options.duckDbConfig().dbMaxTemp()));
            }
        }
    }

    private String sq(String s) {
        return "'" + s + "'";
    }

    private ResourceAnalysisResult analyseResource(ValidationOptions options,
                                                   ResourceDescriptor resource,
                                                   Connection connection,
                                                   DataPackageDescriptor dataPackageDescriptor,
                                                   List<AnalysisFeature> analysisFeatures) throws SQLException {
        List<ForeignKeyViolation> keyViolations = new ArrayList<>();
        List<DataTypeViolation> dataTypeViolations = new ArrayList<>();
        List<ColumnStatistics> columnAnalyses = new ArrayList<>();
        PrimaryKeyViolation primaryKeyViolation = null;
        long rowCount = countRows(connection, resource);

        if (analysisFeatures.contains(AnalysisFeature.FOREIGN_KEY_CONSTRAINT)) {
            List<ForeignKeyViolation> foreignForeignKeyViolations = findForeignKeyViolations(options, resource, connection, dataPackageDescriptor);
            keyViolations.addAll(foreignForeignKeyViolations);
        }

        if (analysisFeatures.contains(AnalysisFeature.PRIMARY_KEY_UNIQUE)) {
            primaryKeyViolation = findPrimaryKeyViolations(options, resource, connection, dataPackageDescriptor);
        }

        if (analysisFeatures.contains(AnalysisFeature.DATA_TYPE_CONSTRAINT)) {
            dataTypeViolations = analyseDataTypeViolations(options, dataPackageDescriptor, connection);
        }

        if (analysisFeatures.contains(AnalysisFeature.COUNT) || analysisFeatures.contains(AnalysisFeature.COUNT_DISTINCT)) {
            for (var field : resource.fields()) {
                ColumnStatistics columnAnalysis = analyseColumn(connection, field, resource);
                columnAnalyses.add(columnAnalysis);
            }
        }

        return new ResourceAnalysisResult(
                resource.name(),
                keyViolations,
                primaryKeyViolation,
                dataTypeViolations,
                columnAnalyses,
                rowCount);
    }

    private PrimaryKeyViolation findPrimaryKeyViolations(ValidationOptions options, ResourceDescriptor resource, Connection connection, DataPackageDescriptor dataPackageDescriptor) throws SQLException {
        if (resource.primaryKey() == null) {
            return null;
        }
        String keyFields = resource.primaryKey().keys().stream()
                .map(key -> q(key))
                .collect(Collectors.joining(", "));

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT COUNT(*), ").append(keyFields);
        sb.append(" FROM ").append(q(resource.name()));
        sb.append(" GROUP BY ").append(keyFields);
        sb.append(" HAVING COUNT(*) > 1");

        String violationSql = sb.toString();

        sb = new StringBuilder();
        sb.append("SELECT COUNT(*)");
        sb.append(" FROM (").append(violationSql).append(")");

        String countSql = sb.toString();
        String sampleSql = violationSql + " LIMIT " + options.sampleSize();

        log.debug("Primary key violation search query: " + violationSql);

        long count;
        try (PreparedStatement statement = connection.prepareStatement(countSql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            count = resultSet.getLong(1);
        }

        if (count == 0) {
            return null;
        }
        List<Map<String, Object>> samples = fetchSampleRows(connection, sampleSql);
        return new PrimaryKeyViolation(resource.name(), resource.primaryKey().keys(), count, samples);
    }

    private List<ForeignKeyViolation> findForeignKeyViolations(
            ValidationOptions options,
            ResourceDescriptor resource,
            Connection connection,
            DataPackageDescriptor dataPackageDescriptor) throws SQLException {
        List<ForeignKeyViolation> foreignKeyViolations = new ArrayList<>();
        for (ForeignKeyDescriptor key : resource.foreignKeys()) {
            log.info("Checking referential integrity for {}[{}]->{}[{}]",
                    resource.name(),
                    String.join(",", key.fields()),
                    key.reference().resource(),
                    String.join(",", key.reference().fields())
            );
            ForeignKeyViolation violation =
                    validateForeignKey(connection, dataPackageDescriptor, resource, key, options.sampleSize());
            if (violation.violationCount() > 0) {
                foreignKeyViolations.add(violation);
            }
        }
        return foreignKeyViolations;
    }

    private long countRows(Connection connection, ResourceDescriptor resource) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + q(resource.name());
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private ColumnStatistics analyseColumn(
            Connection connection,
            FieldDescriptor field,
            ResourceDescriptor resource)
            throws SQLException {
        String sql = createColumnSql(field, resource);

        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            ColumnStatistics columnAnalysis = new ColumnStatistics(
                    field.name(),
                    resultSet.getLong(1),
                    resultSet.getLong(2));
            return columnAnalysis;
        }
    }

    private String createColumnSql(FieldDescriptor field, ResourceDescriptor resource) {
        StringBuilder sb = new StringBuilder();
        sb.append("select");
        sb.append(" COUNT(").append(q(field.name())).append("),");
        sb.append(" COUNT(DISTINCT ").append(q(field.name())).append(")");
        sb.append(" FROM ").append(q(resource.name()));

        String missingValueSql = createMissingValueWhereSql(field);
        if (!missingValueSql.isBlank()) {
            sb.append(" WHERE ").append(missingValueSql);
        }

        return sb.toString();
    }

    private String createMissingValueWhereSql(FieldDescriptor fieldDescriptor) {
        if (fieldDescriptor.missingValues().isEmpty()) {
            return "";
        }

        if (fieldDescriptor.missingValues().size() == 1 ) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        String missingValues = fieldDescriptor.missingValues().stream()
                .map(missingValueDescriptor -> sq(missingValueDescriptor.rawValue()))
                .collect(Collectors.joining(", "));
        sb.append(" ").append(q(fieldname)).append(" not in (").append(missingValues).append(")");
        return sb.toString();
    }

    private ForeignKeyViolation validateForeignKey(
            Connection connection,
            DataPackageDescriptor dataPackage,
            ResourceDescriptor resource,
            ForeignKeyDescriptor key,
            int sampleSize)
            throws SQLException {

        ReferenceDescriptor reference = key.reference();
        String parentName = reference.resource().isBlank() ? resource.name() : reference.resource();
        ResourceDescriptor parentResource = dataPackage.resourcesByName().get(parentName);
        if (parentResource == null) {
            return new ForeignKeyViolation(resource.name(), key.fields(), parentName, reference.fields(), 0L, List.of());
        }

        String countSql = buildViolationCountSql(resource.name(), key.fields(), parentResource.name(), reference.fields());

        long count;
        try (PreparedStatement statement = connection.prepareStatement(countSql);
             ResultSet resultSet = statement.executeQuery()) {
            resultSet.next();
            count = resultSet.getLong(1);
        }

        List<Map<String, Object>> samples =
                count == 0
                        ? List.of()
                        : fetchForeignKeySampleRows(connection, resource.name(), key.fields(), parentResource.name(), reference.fields(), sampleSize);

        return new ForeignKeyViolation(
                resource.name(), key.fields(), parentResource.name(), reference.fields(), count, samples);
    }

    private String buildCountQuerySql(String name, List<String> keys) {
        String sumOfKeysSql = keys.stream()
                .map(key -> "COUNT(" + q(key) + ")")
                .collect(Collectors.joining(", "));

        return "SELECT COUNT(*), " +
                sumOfKeysSql +
                " FROM " +
                q(name);
    }

    private List<Map<String, Object>> fetchForeignKeySampleRows(
            Connection connection,
            String childResource,
            List<String> childFields,
            String parentResource,
            List<String> parentFields,
            int sampleSize)
            throws SQLException {

        String sql = buildSampleSql(childResource, childFields, parentResource, parentFields, sampleSize);
        List<Map<String, Object>> sampleRows = fetchSampleRows(connection, sql);
        return List.copyOf(sampleRows);
    }

    private static List<Map<String, Object>> fetchSampleRows(Connection connection, String sql) throws SQLException {
        List<Map<String, Object>> sampleRows = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql);
             ResultSet resultSet = statement.executeQuery()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row.put(metaData.getColumnName(i), resultSet.getObject(i));
                }
                sampleRows.add(row);
            }
        }
        return sampleRows;
    }

    private static String buildViolationCountSql(
            String childResource,
            List<String> childFields,
            String parentResource,
            List<String> parentFields) {

        return "SELECT COUNT(*) FROM "
                + q(childResource)
                + " c WHERE "
                + childNotNullPredicate(childFields)
                + " AND NOT EXISTS (SELECT 1 FROM "
                + q(parentResource)
                + " p WHERE "
                + equalityJoinPredicate(childFields, parentFields)
                + ")";
    }

    private static String buildSampleSql(
            String childResource,
            List<String> childFields,
            String parentResource,
            List<String> parentFields,
            int sampleSize) {

        return "SELECT "
                + selectChildColumns(childFields)
                + " FROM "
                + q(childResource)
                + " c WHERE "
                + childNotNullPredicate(childFields)
                + " AND NOT EXISTS (SELECT 1 FROM "
                + q(parentResource)
                + " p WHERE "
                + equalityJoinPredicate(childFields, parentFields)
                + ") LIMIT "
                + Math.max(sampleSize, 1);
    }

    private static String childNotNullPredicate(List<String> childFields) {
        StringJoiner joiner = new StringJoiner(" AND ");
        for (String field : childFields) {
            joiner.add("c." + q(field) + " IS NOT NULL");
        }
        return joiner.toString();
    }

    private static String equalityJoinPredicate(List<String> childFields, List<String> parentFields) {
        StringJoiner joiner = new StringJoiner(" AND ");
        for (int i = 0; i < childFields.size(); i++) {
            joiner.add("c." + q(childFields.get(i)) + " = p." + q(parentFields.get(i)));
        }
        return joiner.toString();
    }

    private static String selectChildColumns(List<String> childFields) {
        StringJoiner joiner = new StringJoiner(", ");
        for (String field : childFields) {
            joiner.add("c." + q(field) + " AS " + q(field));
        }
        return joiner.toString();
    }

    static String q(String identifier) {
        if (identifier.length() < 2
                && identifier.substring(0, 1).equalsIgnoreCase("\"")
                && identifier.substring(identifier.length() - 1).equalsIgnoreCase("\"")
        ) {
            return identifier;
        }
        return '"' + identifier.replace("\"", "\"\"") + '"';
    }
}
