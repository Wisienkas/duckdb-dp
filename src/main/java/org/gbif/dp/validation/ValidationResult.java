package org.gbif.dp.validation;

import java.util.List;

public record ValidationResult(
    List<ForeignKeyViolation> foreignKeyViolations,
    List<DataTypeViolation> dataTypeViolations) {

  public boolean isValid() {
    return foreignKeyViolations.isEmpty() && dataTypeViolations.isEmpty();
  }
}
