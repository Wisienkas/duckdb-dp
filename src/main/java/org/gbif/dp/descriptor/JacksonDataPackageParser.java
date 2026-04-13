package org.gbif.dp.descriptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

public class JacksonDataPackageParser implements DataPackageParser {

  private final ObjectMapper mapper;

  public JacksonDataPackageParser() {
    this(new ObjectMapper());
  }

  public JacksonDataPackageParser(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public DataPackageDescriptor parse(Path descriptorPath) throws IOException {
    JsonNode root = mapper.readTree(descriptorPath.toFile());
    String packageName = root.path("name").asText("");

    List<ResourceDescriptor> resources = new ArrayList<>();
    Map<String, ResourceDescriptor> byName = new HashMap<>();

    JsonNode resourceNodes = root.path("resources");
    if (!resourceNodes.isArray()) {
      return new DataPackageDescriptor(packageName, List.of(), Map.of());
    }

    for (JsonNode resourceNode : resourceNodes) {
      String name = resourceNode.path("name").asText("");
      if (name.isBlank()) {
        continue;
      }
      List<Path> paths = new ArrayList<>();
      JsonNode pathNode = resourceNode.path("path");
      if (pathNode.isArray()) {
        StreamSupport.stream(pathNode.spliterator(), false)
                .map(node -> node.asText(""))
                .filter(Predicate.not(String::isBlank))
                .map(str -> descriptorPath.getParent().resolve(str).normalize())
                .forEach(paths::add);
      } else {
        String relativePath = pathNode.asText("");
        if (!relativePath.isBlank()) {
          Path resourcePath = descriptorPath.getParent().resolve(relativePath).normalize();
          paths.add(resourcePath);
        }
      }
      if (paths.isEmpty()) {
        continue;
      }

      JsonNode schemaNode = resourceNode.path("schema");
      List<FieldDescriptor> fields = parseFields(schemaNode.path("fields"));
      List<ForeignKeyDescriptor> foreignKeys = parseForeignKeys(schemaNode.path("foreignKeys"));
      ResourceDescriptor descriptor = new ResourceDescriptor(name, paths, fields, foreignKeys);
      resources.add(descriptor);
      byName.put(name, descriptor);
    }

    return new DataPackageDescriptor(packageName, List.copyOf(resources), Map.copyOf(byName));
  }

  private static List<FieldDescriptor> parseFields(JsonNode fieldsNode) {
    if (!fieldsNode.isArray()) {
      return List.of();
    }

    List<FieldDescriptor> fields = new ArrayList<>();
    for (JsonNode fieldNode : fieldsNode) {
      String fieldName = fieldNode.path("name").asText("").trim();
      String fieldType = fieldNode.path("type").asText("string").trim();
      String fieldFormat = fieldNode.path("format").asText("default").trim();
      if (!fieldName.isBlank()) {
        fields.add(new FieldDescriptor(fieldName, fieldType, fieldFormat));
      }
    }
    return List.copyOf(fields);
  }

  private static List<ForeignKeyDescriptor> parseForeignKeys(JsonNode foreignKeyNodes) {
    if (!foreignKeyNodes.isArray()) {
      return List.of();
    }

    List<ForeignKeyDescriptor> keys = new ArrayList<>();
    for (JsonNode fkNode : foreignKeyNodes) {
      List<String> fields = normalizeToList(fkNode.path("fields"));
      JsonNode referenceNode = fkNode.path("reference");
      String referenceResource = referenceNode.path("resource").asText("");
      List<String> referenceFields = normalizeToList(referenceNode.path("fields"));

      if (!fields.isEmpty() && fields.size() == referenceFields.size()) {
        keys.add(new ForeignKeyDescriptor(fields, new ReferenceDescriptor(referenceResource, referenceFields)));
      }
    }
    return List.copyOf(keys);
  }

  private static List<String> normalizeToList(JsonNode valueNode) {
    if (valueNode == null || valueNode.isMissingNode() || valueNode.isNull()) {
      return List.of();
    }

    if (valueNode.isArray()) {
      List<String> values = new ArrayList<>();
      for (JsonNode item : valueNode) {
        String value = item.asText("").trim();
        if (!value.isBlank()) {
          values.add(value);
        }
      }
      return List.copyOf(values);
    }

    String single = valueNode.asText("").trim();
    return single.isBlank() ? List.of() : List.of(single);
  }
}

