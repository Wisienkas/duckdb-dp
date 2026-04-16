package org.gbif.dp.analysis;

import org.gbif.dp.analysis.model.ColumnAnalysis;
import org.gbif.dp.analysis.model.KeyViolation;

import java.util.List;

public record ResourceAnalysisResult(
            String name,
            List<KeyViolation> keyViolations,
            List<ColumnAnalysis> columnAnalyses,
            long totalRows
        ) {

}
