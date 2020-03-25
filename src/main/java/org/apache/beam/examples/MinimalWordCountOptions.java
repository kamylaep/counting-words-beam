package org.apache.beam.examples;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MinimalWordCountOptions extends PipelineOptions {

  String getSubscription();

  void setSubscription(String subscription);

  String getOutputFilterWordsFileName();

  void setOutputFilterWordsFileName(String outputFilterWordsFileName);

  String getOutputAllWordsFileName();

  void setOutputAllWordsFileName(String outputAllWordsFileName);

  @Default.Long(5)
  long getWindowInSeconds();

  void setWindowInSeconds(long windowInSeconds);

  String getStartsWith();

  void setStartsWith(String filter);
}