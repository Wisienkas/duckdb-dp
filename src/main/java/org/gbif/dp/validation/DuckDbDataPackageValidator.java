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
import org.gbif.dp.descriptor.DataPackageDescriptor;
import org.gbif.dp.descriptor.DataPackageParser;
import org.gbif.dp.descriptor.ForeignKeyDescriptor;
import org.gbif.dp.descriptor.ReferenceDescriptor;
import org.gbif.dp.descriptor.ResourceDescriptor;
import org.gbif.dp.duckdb.DuckDbResourceLoader;

public class DuckDbDataPackageValidator implements DataPackageValidator {

  private final DataPackageParser parser;
  private final DuckDbResourceLoader resourceLoader;
  private final DuckDbDataTypeValidator dataTypeValidator;

  public DuckDbDataPackageValidator(DataPackageParser parser, DuckDbResourceLoader resourceLoader) {
    this(parser, resourceLoader, new DuckDbDataTypeValidator());
  }

  public DuckDbDataPackageValidator(
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

    try (Connection connection = DriverManager.getConnection(options.jdbcUrl())) {
      for (ResourceDescriptor resource : dataPackageDescriptor.resources()) {
        resourceLoader.createResourceView(connection, resource.name(), resource.path());
      }

      // Foreign key validation
      for (ResourceDescriptor resource : dataPackageDescriptor.resources()) {
        for (ForeignKeyDescriptor key : resource.foreignKeys()) {
          ForeignKeyViolation violation =
              validateForeignKey(connection, dataPackageDescriptor, resource, key, options.sampleSize());
          if (violation.violationCount() > 0) {
            fkViolations.add(violation);
          }
        }
      }

      // Data type validation
      for (ResourceDescriptor resource : dataPackageDescriptor.resources()) {
        dtViolations.addAll(dataTypeValidator.validate(connection, resource, options.sampleSize()));
      }
    }

    return new ValidationResult(List.copyOf(fkViolations), List.copyOf(dtViolations));
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
            : fetchSampleRows(connection, resource.name(), key.fields(), parentResource.name(), reference.fields(), sampleSize);

    return new ForeignKeyViolation(
        resource.name(), key.fields(), parentResource.name(), reference.fields(), count, samples);
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
