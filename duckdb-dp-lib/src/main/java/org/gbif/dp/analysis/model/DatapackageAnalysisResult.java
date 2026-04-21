package org.gbif.dp.analysis.model;

import org.gbif.dp.analysis.ResourceAnalysisResult;

import java.util.List;
import java.util.Objects;

public record DatapackageAnalysisResult(
        List<ResourceAnalysisResult> resourceAnalysisResults
    ) {

  public boolean isValid() {
      return resourceAnalysisResults.stream()
              .allMatch(ResourceAnalysisResult::isValid);
  }

    public List<ForeignKeyViolation> foreignKeyViolations() {
        return resourceAnalysisResults.stream()
                .flatMap(rar -> rar.foreignKeyViolations().stream())
                .toList();
    }

    public List<PrimaryKeyViolation> primaryKeyViolations() {
      return resourceAnalysisResults.stream()
              .map(ResourceAnalysisResult::primaryKeyViolation)
              .filter(Objects::nonNull)
              .toList();
    }

    public List<DataTypeViolation> dataTypeViolations() {
        return resourceAnalysisResults.stream()
                .flatMap(rar -> rar.dataTypeViolations().stream())
                .toList();
    }
}
