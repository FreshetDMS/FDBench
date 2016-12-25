#!/bin/sh
export JMX_PORT=${JMX_PORT:-9999}
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname={{ inventory_hostname }} -Djava.net.preferIPv4Stack=true"
echo -e "Starting 9092 Server"
nohup sh {{ common.soft_link_base_path }}/kafka/bin/kafka-server-start.sh {{ common.soft_link_base_path }}/kafka/config/server.properties &
