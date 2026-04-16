package org.gbif.dp.analysis.model;

public record ColumnAnalysis(
        String name,
        long populatedValues,
        long uniqueValues
) {
}
