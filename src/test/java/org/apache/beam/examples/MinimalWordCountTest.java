package org.apache.beam.examples;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.beam.examples.MinimalWordCount.ProcessStrings;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PubsubIO.class, TextIO.class})
public class MinimalWordCountTest implements Serializable {

  private static final List<String> EXPECTED_ALL_WORDS_COUNT = Arrays
    .asList("I: 1", "am: 1", "a: 2", "test: 2", "so: 1", "me: 1", "please: 1", "another: 1", "word: 1", "with: 1", "the: 1", "letter: 1");
  private static final List<String> EXPECTED_FILTERED_WORDS_COUNT = Arrays.asList("am: 1", "a: 2", "another: 1");

  @Rule
  public final transient TestPipeline testPipeline;

  private MinimalWordCountOptions options;

  public MinimalWordCountTest() {
    options = buildOptions();
    testPipeline = TestPipeline.fromOptions(options);
  }

  @After
  public void cleanUp() {
    FileUtils.deleteQuietly(new File(options.getOutputPath()));
  }

  @Test
  public void shouldProduceTwoFilesWithTheCountedWords() throws Exception {
    testPipeline
      .apply(Create.of(Arrays.asList("I", "am", "a", "test", "so", "test", "me", "please", "another", "word", "with", "the", "letter", "a")))
      .apply(new ProcessStrings(options.getWindowInSeconds(), options.getStartsWith(), options.getOutputPath()));

    testPipeline.run().waitUntilFinish();

    assertData("all", EXPECTED_ALL_WORDS_COUNT);
    assertData("filtered", EXPECTED_FILTERED_WORDS_COUNT);
  }

  private void assertData(String outputFileName, List<String> expectedData) throws IOException {
    //TODO assert number of shards X number of files
    List<String> words = Files.walk(Paths.get(options.getOutputPath()))
      .filter(p -> p.getFileName().toString().startsWith(outputFileName))
      .flatMap(this::readFileToStream)
      .collect(Collectors.toList());
    Assert.assertTrue(words.containsAll(expectedData));
  }

  private Stream<? extends String> readFileToStream(Path p) {
    try {
      return Files.readAllLines(p).stream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private MinimalWordCountOptions buildOptions() {
    MinimalWordCountOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(MinimalWordCountOptions.class);
    pipelineOptions.setOutputPath("/tmp/" + UUID.randomUUID().toString());
    pipelineOptions.setStartsWith("a");
    return pipelineOptions;
  }

}
