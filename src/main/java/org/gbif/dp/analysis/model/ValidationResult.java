package org.gbif.dp.analysis.model;

import java.util.List;

public record ValidationResult(
        List<ResourceDescription> ResourceAnalysisState,
        List<KeyViolation> keyViolations,
        List<DataTypeViolation> dataTypeViolations
    ) {

  public boolean isValid() {
    return keyViolations.isEmpty() && dataTypeViolations.isEmpty();
  }
}
