# sessionize-sample
Sessionize access logs using Spark

## Format

access logs

`timestamp ipAddress`

```
2016-04-15T00:00:01 203.0.113.0
2016-04-15T00:00:22 203.0.113.42
2016-04-15T00:05:01 203.0.113.2
2016-04-15T00:28:01 203.0.113.0
...
```

output

`ipAddress accessCount sojournTime(seconds)`

```
203.0.113.0 2 1680
203.0.113.42 1 0
203.0.113.2 1 0
...
```

## Usage

```bash
$ export SPARK_HOME=/path/to/spark
$ export INPUT_FILE=/path/to/access_logs
$ export OUTPUT_FILE=/path/to/output
$ sbt assembly
$ $SPARK_HOME/bin/spark-submit --master "local[2]" --name "sessionize" /path/to/assembly.jar
```
