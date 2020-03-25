package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class MinimalWordCount {

  public static void main(String[] args) {
    PipelineOptionsFactory.register(MinimalWordCountOptions.class);
    MinimalWordCountOptions options = PipelineOptionsFactory.fromArgs(args).create().as(MinimalWordCountOptions.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
      .apply("ReadData", PubsubIO.readStrings().fromSubscription(options.getSubscription()))
      .apply("ProcessString", new ProcessStrings(options.getWindowInSeconds(), options.getStartsWith(), options.getOutputPath()));
    pipeline.run();
  }

  public static class ProcessStrings extends PTransform<PCollection<String>, PDone> {

    private long windowInSeconds;
    private String startsWith;
    private String outputPath;

    public ProcessStrings(long windowInSeconds, String startsWith, String outputPath) {
      this.windowInSeconds = windowInSeconds;
      this.startsWith = startsWith;
      this.outputPath = outputPath;
    }

    @Override
    public PDone expand(PCollection<String> input) {
      PCollection<KV<String, Long>> counted = input
        .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(windowInSeconds))))
        .apply("CountWords", Count.perElement());

      counted
        .apply("MapAllWordsToString", MapElements.into(TypeDescriptors.strings()).via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
        .apply("WriteAllWords", TextIO.write().to(outputPath + "/all").withWindowedWrites().withNumShards(2));

      counted
        .apply(Filter.by(word -> word.getKey().startsWith(startsWith)))
        .apply("MapFilteredWordsToString", MapElements.into(TypeDescriptors.strings()).via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
        .apply("WriteFilteredWords", TextIO.write().to(outputPath + "/filtered").withWindowedWrites().withNumShards(2));

      return PDone.in(counted.getPipeline());
    }
  }

}
