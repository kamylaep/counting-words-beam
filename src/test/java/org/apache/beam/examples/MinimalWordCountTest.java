package org.apache.beam.examples;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.io.FileUtils;
import org.junit.After;
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
    FileUtils.deleteQuietly(new File(options.getOutputAllWordsFileName()));
    FileUtils.deleteQuietly(new File(options.getOutputFilterWordsFileName()));
  }

  @Test
  public void shouldProduceTwoFilesWithTheCountedWords() throws Exception {
    PCollection<String> input = testPipeline.apply(Create.of(Arrays.asList("I", "am", "a", "test", "so", "test", "me", "please", "another", "word", "with", "the", "letter", "a")));
    MinimalWordCount.buildPipeline(options, input);
    testPipeline.run().waitUntilFinish();
    assertData(options.getOutputAllWordsFileName(), EXPECTED_ALL_WORDS_COUNT);
    assertData(options.getOutputFilterWordsFileName(), EXPECTED_FILTERED_WORDS_COUNT);
  }

  private void assertData(String outputFileName, List<String> expectedData) throws IOException {
    Files.readAllLines(Paths.get(outputFileName)).containsAll(expectedData);
  }

  private MinimalWordCountOptions buildOptions() {
    MinimalWordCountOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(MinimalWordCountOptions.class);
    pipelineOptions.setOutputAllWordsFileName("/tmp/" + UUID.randomUUID().toString() + ".txt");
    pipelineOptions.setOutputFilterWordsFileName("/tmp/" + UUID.randomUUID().toString() + ".txt");
    pipelineOptions.setStartsWith("a");
    return pipelineOptions;
  }

}
