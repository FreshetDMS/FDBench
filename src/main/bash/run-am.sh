#!/usr/bin/env bash

# Check if server is set. If not - set server optimization
[[ $JAVA_OPTS != *-server* ]] && export JAVA_OPTS="$JAVA_OPTS -server"

# Set container name system properties for use in Log4J
[[ $JAVA_OPTS != *-Dkbench.container.name* ]] && export JAVA_OPTS="$JAVA_OPTS -Dkbench.container.name=kbench-application-master"

exec $(dirname $0)/run-class.sh org.pathirage.kafka.bench.KBenchAppMaster "$@"
