# counting-words-beam

Simple example of an Apache Beam pipeline that reads messages from GCP Pub/Sub, count all the words and the words starting with the value of the parameter `startsWith` and save the results 
to Google Cloud Storage. 

## Executing

```shell script
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.MinimalWordCount -Dexec.args=" \
--runner=DataflowRunner \
--streaming=true \
--project=<PROJETCT-ID> \
--subscription=<SUBSCRIPTION> \
--startsWith=a \
--outputPath=<OUTPUT-PATH> \
-Pdataflow-runner
```
