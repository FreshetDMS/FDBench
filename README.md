# FDBench

FDBench is a fast data management benchmark.

# How To Use

## Build and Install

Run following command while in FDBench root directory to setup the local environment for development and testing.

```
$ ./bin/localcluster install all
```

## Start YARN and Kafka

```
$ ./bin/localcluster start all
```

After starting YARN and Kafka set the environment variable ```HADOOP_CONF_DIR``` to point to ```$FDBENCH_ROOT/deploy/yarn/etc/hadoop```. This tells FDBench job runner where it can find the YARN cluster configuration. 

*If you want to use one of the existing YARN clusters, please make sure to point ```HADOOP_CONF_DIR```  environment variable to correct YARN configuration directory*

## Executing Built-In Kafka Benchmark

You can run built-in Kafka workload generator sample by executing following command:

```
$ ./deploy/fdbench/bin/run-bench.sh --config-file ./deploy/fdbench/examples/synthetic-workloadgen.conf
```


Check whether the workload generator is running by going to http://localhost:8088.

## Enabling Metrics Reporter

You can enable file system metrics reporter by adding following code block to your job configuration.

```
metrics.reporters="fs"
metrics.reporter.fs.factory.class="org.pathirage.fdbench.metrics.FSMetricsSnapshotReporterFactory"
metrics.reporter.fs.snapshots.directory=<directory-to-put-metrics>
```