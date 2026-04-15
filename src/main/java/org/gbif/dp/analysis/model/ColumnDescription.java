package org.gbif.dp.analysis.model;

public record ColumnDescription(
        String name,
        long populatedValues,
        long uniqueValues
) {
}
