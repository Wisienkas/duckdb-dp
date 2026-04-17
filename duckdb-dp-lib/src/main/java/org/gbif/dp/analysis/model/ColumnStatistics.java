package org.gbif.dp.analysis.model;

public record ColumnStatistics(
        String name,
        long populatedValues,
        long uniqueValues
) {
}
