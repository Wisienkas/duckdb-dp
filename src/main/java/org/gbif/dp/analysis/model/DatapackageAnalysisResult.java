package org.gbif.dp.analysis.model;

import org.gbif.dp.analysis.ResourceAnalysisResult;

import java.util.List;

public record DatapackageAnalysisResult(
        List<ResourceAnalysisResult> resourceAnalysisResults,
        List<DataTypeViolation> dataTypeViolations
    ) {

  public boolean isValid() {
      boolean validKeys = resourceAnalysisResults.stream().allMatch(rar -> rar.keyViolations().isEmpty());
      return validKeys && dataTypeViolations.isEmpty();
  }

    public List<KeyViolation> keyViolations() {
        return resourceAnalysisResults.stream()
                .flatMap(rar -> rar.keyViolations().stream())
                .toList();
    }
}
