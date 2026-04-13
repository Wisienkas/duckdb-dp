package org.gbif.dp.validation;

public record ColumnDescription(
        String name,
        long populatedValues,
        long uniqueValues
) {
}
