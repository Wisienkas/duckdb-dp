package org.gbif.dp.analysis;

import org.gbif.dp.analysis.model.DatapackageAnalysisResult;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;

public interface DataPackageAnalyser {

  DatapackageAnalysisResult analyse(Path descriptorPath, ValidationOptions options, List<AnalysisFeature> analysisFeatures)
          throws IOException, SQLException;
}

