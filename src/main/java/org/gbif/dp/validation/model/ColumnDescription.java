package org.gbif.dp.validation.model;

public record ColumnDescription(
        String name,
        long populatedValues,
        long uniqueValues
) {
}
