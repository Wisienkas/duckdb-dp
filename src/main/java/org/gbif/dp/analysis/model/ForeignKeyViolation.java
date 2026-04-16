package org.gbif.dp.analysis.model;

import java.util.List;
import java.util.Map;

public record ForeignKeyViolation(
    String resource,
    List<String> fields,
    String referenceResource,
    List<String> referenceFields,
    long violationCount,
    List<Map<String, Object>> sampleRows) {}

