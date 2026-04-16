package org.gbif.dp.analysis.model;

import org.gbif.dp.analysis.ResourceAnalysisResult;

import java.util.List;

public record DatapackageAnalysisResult(
        List<ResourceAnalysisResult> resourceAnalysisResults
    ) {

  public boolean isValid() {
      boolean validKeys = resourceAnalysisResults.stream().allMatch(rar -> rar.foreignKeyViolations().isEmpty());
      boolean validDataTypes = resourceAnalysisResults.stream().allMatch(rar -> rar.dataTypeViolations().isEmpty());
      return validKeys && validDataTypes;
  }

    public List<ForeignKeyViolation> keyViolations() {
        return resourceAnalysisResults.stream()
                .flatMap(rar -> rar.foreignKeyViolations().stream())
                .toList();
    }

    public List<DataTypeViolation> dataTypeViolations() {
        return resourceAnalysisResults.stream()
                .flatMap(rar -> rar.dataTypeViolations().stream())
                .toList();
    }
}
