package org.gbif.dp.analysis;

import org.gbif.dp.analysis.model.ValidationResult;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

public interface DataPackageValidator {

  ValidationResult validate(Path descriptorPath, ValidationOptions options) throws IOException, SQLException;
}

