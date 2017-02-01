#!/usr/bin/env bash

# Set HADOOP_CONF if not set
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-~/.fdbench/conf}


exec $(dirname $0)/run-class.sh org.pathirage.fdbench.BenchRunner "$@"
