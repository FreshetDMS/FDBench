#!/usr/bin/env bash

ZK_IP=$(<~/.zkip)
KAFKA_IP=$(<~/.kafkaip)


cat > ~/kafka/config/server.properties << EOF
broker.id=0
delete.topic.enable=true

hosts.name=0.0.0.0
advertised.hosts.name=$KAFKA_IP
listeners=PLAINTEXT://0.0.0.0:9092
advertised.listeners=PLAINTEXT://$KAFKA_IP:9092

num.network.threads=3
num.io.threads=8

socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/mnt/data
num.partitions=1
num.recovery.threads.per.data.dir=1

# The number of messages to accept before forcing a flush of data to disk
#log.flush.interval.messages=10000

# The maximum amount of time a message can sit in a log before we force a flush
#log.flush.interval.ms=1000

log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

zookeeper.connect=$ZK_IP:2181
zookeeper.connection.timeout.ms=6000
EOF

cd ~/kafka

nohup ./bin/kafka-server-start.sh config/server.properties &
