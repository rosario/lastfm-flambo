Build with:

```> lein uberjar```

A standalone jar called `spark-example-0.1.0-SNAPSHOT-standalone.jar` would be created.

The app takes two parameters the `inputfile` and the output folder. Example

```
./spark-2.2.0-bin-hadoop2.7/bin/spark-submit --class lastfm.core spark-example-0.1.0-SNAPSHOT-standalone.jar lastfm-dataset-1K/userid-timestamp-artid-artname-traid-traname.tsv ./output-data
```