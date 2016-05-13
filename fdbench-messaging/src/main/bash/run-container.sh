#!/usr/bin/env bash

# Check if server is set. If not - set server optimization
[[ $JAVA_OPTS != *-server* ]] && export JAVA_OPTS="$JAVA_OPTS -server"

# Set container ID system property for use in Log4J
[[ $JAVA_OPTS != *-Dkbench.container.id* && ! -z "$KBENCH_CONTAINER_ID" ]] && export JAVA_OPTS="$JAVA_OPTS -Dkbench.container.id=$KBENCH_CONTAINER_ID"

# Set container name system property for use in Log4J
[[ $JAVA_OPTS != *-Dkbench.container.name* && ! -z "$KBENCH_CONTAINER_ID" ]] && export JAVA_OPTS="$JAVA_OPTS -Dkbench.container.name=kbench-container-$KBENCH_CONTAINER_ID"

exec $(dirname $0)/run-class.sh org.pathirage.fdbench.messaging.yarn.FDMessagingBenchContainer "$@"
