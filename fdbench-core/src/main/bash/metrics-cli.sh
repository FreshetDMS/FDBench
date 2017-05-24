#!/usr/bin/env bash

exec $(dirname $0)/run-class.sh org.pathirage.fdbench.metrics.KafkaMetricsSnapshotConsumerCLI "$@"