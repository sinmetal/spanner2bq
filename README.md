# spanner2bq

```
 mvn compile exec:java -Dexec.mainClass=org.sinmetal.spanner2bq.SpannerToBigQuery -Dexec.args="--runner=DataflowRunner --project={your dataflow project} \
                  --gcpTempLocation=gs://{your bucket}/tmp \
                  --spannerProjectId={your spanner project id} --inputSpannerInstanceId={your spanner instance} --inputSpannerDatabaseId={your spanner database }" -Pdataflow-runner
```

現状、BigQuery dataset名は `spanner` で固定されてるので、これも指定できるようにする。