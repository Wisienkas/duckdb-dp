package org.gbif.dp.validation.model;

import java.util.List;
import java.util.Map;

public record KeyViolation(
    String resource,
    List<String> fields,
    String referenceResource,
    List<String> referenceFields,
    long violationCount,
    List<Map<String, Object>> sampleRows) {}

