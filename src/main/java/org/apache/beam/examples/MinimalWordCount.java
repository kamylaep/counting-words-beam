package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class MinimalWordCount {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(MinimalWordCountOptions.class);
    MinimalWordCountOptions options = PipelineOptionsFactory.fromArgs(args).create().as(MinimalWordCountOptions.class);

    Pipeline p = Pipeline.create(options);
    PCollection<String> inputData = p.apply("ReadData", PubsubIO.readStrings().fromSubscription(options.getSubscription()));
    buildPipeline(options, inputData);
    p.run();
  }

  protected static void buildPipeline(MinimalWordCountOptions options, PCollection<String> inputData) {
    PCollection<KV<String, Long>> counted = inputData
        .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(options.getWindowInSeconds()))))
        .apply("CountWords", Count.perElement());

    counted
        .apply("MapAllWordsToString", MapElements.into(TypeDescriptors.strings()).via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
        .apply("WriteAllWords", TextIO.write().to(options.getOutputAllWordsFileName()).withWindowedWrites().withoutSharding());

    String startsWith = options.getStartsWith();
    counted
        .apply(Filter.by(word -> word.getKey().startsWith(startsWith)))
        .apply("MapFilteredWordsToString", MapElements.into(TypeDescriptors.strings()).via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
        .apply("WriteFilteredWords", TextIO.write().to(options.getOutputFilterWordsFileName()).withWindowedWrites().withoutSharding());
  }

}
