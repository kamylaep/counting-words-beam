# counting-words-beam

Simple example of an Apache Beam pipeline that reads messages from GCP Pub/Sub, count all the words and the words starting with the value o the parameter `startsWith` and save the results 
to Google Cloud Storage. 

## Executing

```shell script
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.MinimalWordCount -Dexec.args=" \
--runner=DataflowRunner \
--streaming=true \
--project=<PROJETCT-ID> \
--subscription=<SUBSCRIPTION> \
--startsWith=a \
--outputFilterFileName=<OUTPUT-PATH-FOR-WORDS-STARTING-WITH-A> \
--outputAllFileName=<OUTPUT-PATH-FOR-ALL-WORDS> \
-Pdataflow-runner
```
