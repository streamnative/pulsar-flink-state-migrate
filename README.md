# pulsar-flink-state-migrate
pulsar flink state migrate tools

## Usage

```
Usage:
	-uid                flink uid for pulsar source
	-savepointPath      flink savepoint path
	-newStatePath      new flink savepoint path
```


This tool is used to migrate from pulsar-flink-connector versions prior to `2.4.27` to [pulsar-flink-connector version `2.5.8.3`](https://repo1.maven.org/maven2/io/streamnative/connectors/).
At the same time, the Flink client version will be upgraded from `1.9.x` to `1.11.x`, the Pulsar client version will be upgraded from `2.5.2` to `2.7.0`.

This upgrade tool does not support batch flink job upgrade. When you need batch, please call it externally from batch.
It needs to be submitted as a Flink job for upgrade.

example:
`bin/flink run pulsar-flink-state-migrate-1.11-0.0.1.jar -uid pulsar-source-id -savepointPath savepoint/savepoint-1af2b3-a7db17bbd4b1 -newStatePath /new-savepoint`

`uid` is the uid set for Pulsar Source in the user code.
`savepointPath` parameter is the old savepoint directory or flink state`_metadata` file
The `newStatePath` parameter is the output directory, where the `_metadata` file will be generated
