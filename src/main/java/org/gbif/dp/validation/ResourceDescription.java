package org.gbif.dp.validation;

import java.util.List;

public record ResourceDescription(
        String name,
        List<ForeignKeyViolation> foreignKeyViolations,
        List<ColumnDescription> columnDescriptions,
        long totalRows

) {
}
