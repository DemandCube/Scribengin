#Set up docker images
DOCKERSCRIBEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
$DOCKERSCRIBEDIR/../docker.sh cluster --clean-containers --run-containers --deploy-scribengin --start-cluster

#make folder for test results
mkdir testresults

#Give everything time to come up
sleep 5

ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin info"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm info"



#Run dataflow
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && \
          ./bin/shell.sh dataflow-test kafka-to-kafka \
             --dataflow-id    kafka-to-kafka-1 \
             --dataflow-name  kafka-to-kafka \
             --worker 3 \
             --executor-per-worker 1 \
             --duration 90000 \
             --task-max-execute-time 1000 \
             --source-name input \
             --source-num-of-stream 10 \
             --source-write-period 0 \
             --source-max-records-per-stream 10000 \
             --sink-name output \
             --print-dataflow-info -1 \
             --debug-dataflow-task \
             --debug-dataflow-vm \
             --debug-dataflow-activity \
             --junit-report KafkaIntegrationTest.xml \
             --dump-registry "


#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/KafkaIntegrationTest.xml ./testresults/

#Clean up
$DOCKERSCRIBEDIR/.././docker.sh cluster --clean-containers

