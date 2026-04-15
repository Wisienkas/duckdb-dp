package org.gbif.dp.analysis.model;

import java.util.List;

public record ResourceDescription(
        String name,
        List<KeyViolation> keyViolations,
        List<ColumnDescription> columnDescriptions,
        long totalRows

) {
}
