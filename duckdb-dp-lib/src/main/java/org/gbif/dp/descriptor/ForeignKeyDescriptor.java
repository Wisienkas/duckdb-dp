package org.gbif.dp.descriptor;

import java.util.List;

public record ForeignKeyDescriptor(List<String> fields, ReferenceDescriptor reference) {}

