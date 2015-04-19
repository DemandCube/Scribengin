#Set up docker images
DOCKERSCRIBEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
$DOCKERSCRIBEDIR/../startCluster.sh

#make folder for test results
mkdir testresults

#Start cluster
ssh -o StrictHostKeyChecking=no neverwinterdp@hadoop-master "cd /opt/cluster && python clusterCommander.py cluster --start --clean status"

sleep 5
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin info"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm info"


#Run dataflow
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && \
                              ./bin/shell.sh dataflow-test kafka-hdfs --dataflow-name kafka-hdfs   \
                              --worker 3 --executor-per-worker 1 --duration 100000 --task-max-execute-time 50000 \
                              --source-name input --source-num-of-stream 10 --source-write-period 5 \
                              --source-max-records-per-stream 1000 --sink-name input  --print-dataflow-info -1 \
                              --debug-dataflow-task true --debug-dataflow-worker true \
                              --junit-report Kafka_to_HDFS_IntegrationTest.xml --dump-registry"

#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/Kafka_to_HDFS_IntegrationTest.xml ./testresults/

#Clean up
$DOCKERSCRIBEDIR/../stopCluster.sh
