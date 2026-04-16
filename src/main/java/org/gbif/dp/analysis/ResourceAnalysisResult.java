package org.gbif.dp.analysis;

import org.gbif.dp.analysis.model.ColumnStatistics;
import org.gbif.dp.analysis.model.DataTypeViolation;
import org.gbif.dp.analysis.model.ForeignKeyViolation;

import java.util.List;

public record ResourceAnalysisResult(
        String name,
        List<ForeignKeyViolation> foreignKeyViolations,
        org.gbif.dp.analysis.model.PrimaryKeyViolation primaryKeyViolation, List<DataTypeViolation> dataTypeViolations,
        List<ColumnStatistics> columnAnalyses,
        long totalRows
        ) {

}
