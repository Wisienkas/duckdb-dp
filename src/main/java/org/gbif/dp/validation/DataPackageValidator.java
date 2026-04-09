package org.gbif.dp.validation;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;

public interface DataPackageValidator {

  ValidationResult validate(Path descriptorPath, ValidationOptions options) throws IOException, SQLException;
}

