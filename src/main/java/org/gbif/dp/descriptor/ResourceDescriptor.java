package org.gbif.dp.descriptor;

import java.nio.file.Path;
import java.util.List;

public record ResourceDescriptor(
    String name,
    List<Path> paths,
    List<FieldDescriptor> fields,
    List<ForeignKeyDescriptor> foreignKeys) {}
