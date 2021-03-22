# pulsar-flink-state-migrate
pulsar flink state migrate tools

## Usage

```
Usage:
	-uid                flink uid for pulsar source
	-savepointPath      flink savepoint path
	-newStatePath      new flink savepoint path
```
example:
`bin/flink run pulsar-flink-state-migrate-1.11-0.0.1.jar -uid pulsar-source-id -savepointPath savepoint/savepoint-1af2b3-a7db17bbd4b1 -newStatePath /new-savepoint`
