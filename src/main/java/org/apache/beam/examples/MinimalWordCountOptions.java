package org.apache.beam.examples;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MinimalWordCountOptions extends PipelineOptions {

  String getSubscription();

  void setSubscription(String subscription);

  String getOutputPath();

  void setOutputPath(String outputPath);

  @Default.Long(5)
  long getWindowInSeconds();

  void setWindowInSeconds(long windowInSeconds);

  String getStartsWith();

  void setStartsWith(String filter);
}