#!/bin/bash


bin=`dirname "$0"`
bin=`cd "$bin"; pwd`


$bin/shell.sh vm start
$bin/shell.sh scribengin start
$bin/shell.sh dataflow-test kafka \
    --worker 3 --executor-per-worker 1 --duration 70000 --task-max-execute-time 1000 \
    --kafka-num-partition 10 --kafka-write-period 5 --kafka-max-message-per-partition 3000
