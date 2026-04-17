package org.gbif.dp.analysis;

import java.util.List;
import java.util.stream.Stream;

public enum AnalysisFeature {
    COUNT,
    COUNT_DISTINCT,
    FOREIGN_KEY_CONSTRAINT,
    PRIMARY_KEY_UNIQUE,
    DATA_TYPE_CONSTRAINT,
    ENUM_VALUES_CONSTRAINT;

    public static final List<AnalysisFeature> ALL_FEATURES = Stream.of(AnalysisFeature.values()).toList();
}
