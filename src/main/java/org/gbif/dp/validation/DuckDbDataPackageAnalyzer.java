package org.gbif.dp.validation;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.gbif.dp.descriptor.*;
import org.gbif.dp.duckdb.DuckDbResourceLoader;

public class DuckDbDataPackageAnalyzer implements DataPackageValidator {

  private final DataPackageParser parser;
  private final DuckDbResourceLoader resourceLoader;
  private final DuckDbDataTypeValidator dataTypeValidator;

  public DuckDbDataPackageAnalyzer(DataPackageParser parser, DuckDbResourceLoader resourceLoader) {
    this(parser, resourceLoader, new DuckDbDataTypeValidator());
  }

  public DuckDbDataPackageAnalyzer(
      DataPackageParser parser,
      DuckDbResourceLoader resourceLoader,
      DuckDbDataTypeValidator dataTypeValidator) {
    this.parser = parser;
    this.resourceLoader = resourceLoader;
    this.dataTypeValidator = dataTypeValidator;
  }

  @Override
  public ValidationResult validate(Path descriptorPath, ValidationOptions options)
      throws IOException, SQLException {
    DataPackageDescriptor dataPackageDescriptor = parser.parse(descriptorPath);
    List<ForeignKeyViolation> fkViolations = new ArrayList<>();
    List<DataTypeViolation> dtViolations = new ArrayList<>();
    Map<String, ResourceDescription> resourceDescriptions = new HashMap<>();

    try (Connection connection = DriverManager.getConnection(options.jdbcUrl())) {
      for (ResourceDescriptor resource : dataPackageDescriptor.resources()) {
        System.out.printf("Creating view for %s -> %s%n", resource.name(), resource.path());
        resourceLoader.createResourceView(connection, resource.name(), resource.path());
      }

      // Foreign key validation
      for (ResourceDescriptor resource : dataPackageDescriptor.resources()) {
        analyseResource(options, resource, connection, dataPackageDescriptor, resourceDescriptions, fkViolations);
      }

      // Data type validation
      for (ResourceDescriptor resource : dataPackageDescriptor.resources()) {
        dtViolations.addAll(dataTypeValidator.validate(connection, resource, options.sampleSize()));
      }

    }

    return new ValidationResult(
            List.copyOf(resourceDescriptions.values()),
            List.copyOf(fkViolations),
            List.copyOf(dtViolations));
  }

  private void analyseResource(ValidationOptions options, ResourceDescriptor resource, Connection connection, DataPackageDescriptor dataPackageDescriptor, Map<String, ResourceDescription> resourceDescriptions, List<ForeignKeyViolation> fkViolations) throws SQLException {
    List<ForeignKeyViolation> foreignKeyViolations = new ArrayList<>();
    List<ColumnDescription> columnDescriptions = new ArrayList<>();
    for (ForeignKeyDescriptor key : resource.foreignKeys()) {
      System.out.printf("Checking referential integrity for %s[%s]->%s[%s]%n",
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
    for (var field : resource.fields()) {
      ColumnDescription columnDescription = analyseColumn(connection, field, resource);
      columnDescriptions.add(columnDescription);
    }
    long rowCount = countRows(connection, resource);
    resourceDescriptions.put(resource.name(), new ResourceDescription(
            resource.name(),
            foreignKeyViolations,
            columnDescriptions,
            rowCount));

    fkViolations.addAll(foreignKeyViolations);
  }

  private long countRows(Connection connection, ResourceDescriptor resource) throws SQLException {
      String sql = "SELECT COUNT(*) FROM " + q(resource.name());
      try (PreparedStatement statement = connection.prepareStatement(sql);
           ResultSet resultSet = statement.executeQuery()) {
        resultSet.next();
        return resultSet.getLong(1);
      }
    }

  private ColumnDescription analyseColumn(
            Connection connection,
            FieldDescriptor field,
            ResourceDescriptor resource)
            throws SQLException {
        String sql = createColumnSql(field, resource);

      try (PreparedStatement statement = connection.prepareStatement(sql);
           ResultSet resultSet = statement.executeQuery()) {
        resultSet.next();
        ColumnDescription columnDescription = new ColumnDescription(
                field.name(),
                resultSet.getLong(1),
                resultSet.getLong(2));
        return columnDescription;
      }
    }

    private String createColumnSql(FieldDescriptor field, ResourceDescriptor resource) {
        StringBuilder sb = new StringBuilder();
        sb.append("select");
        sb.append(" COUNT(").append(q(field.name())).append("),");
        sb.append(" COUNT(DISTINCT ").append(q(field.name())).append(")");
        sb.append(" FROM ").append(q(resource.name()));

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
    String fullCountSql = buildCountQuerySql(resource.name(), key.fields());
    try (PreparedStatement statement = connection.prepareStatement(fullCountSql);
         ResultSet resultSet = statement.executeQuery()) {
      resultSet.next();

      StringBuilder sb = new StringBuilder()
              .append("counting rows and relevant rows for ")
              .append(resource.name())
              .append("(").append(resultSet.getLong(1)).append(")")
              .append("[");
      for (int i = 0; i < key.fields().size(); i++) {
        String fieldName = key.fields().get(i);
        Long count = resultSet.getLong(i + 2);
        sb.append("{ ").append(fieldName).append(" -> ").append(count).append("}");
      }

      System.out.println(sb);
    }

    long count;
    try (PreparedStatement statement = connection.prepareStatement(countSql);
        ResultSet resultSet = statement.executeQuery()) {
      resultSet.next();
      count = resultSet.getLong(1);
    }

    List<Map<String, Object>> samples =
        count == 0
            ? List.of()
            : fetchSampleRows(connection, resource.name(), key.fields(), parentResource.name(), reference.fields(), sampleSize);

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

  private List<Map<String, Object>> fetchSampleRows(
      Connection connection,
      String childResource,
      List<String> childFields,
      String parentResource,
      List<String> parentFields,
      int sampleSize)
      throws SQLException {

    String sql = buildSampleSql(childResource, childFields, parentResource, parentFields, sampleSize);
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
    return List.copyOf(sampleRows);
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

  private static String q(String identifier) {
    return '"' + identifier.replace("\"", "\"\"") + '"';
  }
}
