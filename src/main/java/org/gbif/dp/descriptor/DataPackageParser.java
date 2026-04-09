package org.gbif.dp.descriptor;

import java.io.IOException;
import java.nio.file.Path;

public interface DataPackageParser {

  DataPackageDescriptor parse(Path descriptorPath) throws IOException;
}

