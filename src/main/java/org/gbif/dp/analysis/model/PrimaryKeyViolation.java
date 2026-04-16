package org.gbif.dp.analysis.model;

import java.util.List;
import java.util.Map;

public record PrimaryKeyViolation(
    String resource,
    List<String> fields,
    long violationCount,
    List<Map<String, Object>> sampleRows) {}

