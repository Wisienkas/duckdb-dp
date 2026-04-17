package org.gbif.dp.descriptor;

import java.util.List;
import java.util.Map;

public record DataPackageDescriptor(
    String name,
    List<ResourceDescriptor> resources,
    Map<String, ResourceDescriptor> resourcesByName) {}

