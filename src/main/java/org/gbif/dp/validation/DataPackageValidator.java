package org.gbif.dp.validation;

import org.gbif.dp.validation.model.ValidationResult;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

public interface DataPackageValidator {

  ValidationResult validate(Path descriptorPath, ValidationOptions options) throws IOException, SQLException;
}

