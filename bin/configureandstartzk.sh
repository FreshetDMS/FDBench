#!/usr/bin/env bash

cat > ~/zk/config/zoo.cfg << EOF
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/tmp/zookeeper
clientPort=2181
EOF

cd ~/zk

nohup ./bin/zkServer.sh start &