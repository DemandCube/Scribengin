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
ssh  -o StrictHostKeyChecking=no neverwinterdp@hadoop-master \
  "mkdir -p /opt/junit-reports/ && cd /opt/scribengin/scribengin && \
   nohup ./bin/shell.sh dataflow-test kafka-to-kakfa \
            --dataflow-name  kafka-to-kafka \
            --worker 2 --executor-per-worker 2 \
            --duration 600000 --task-max-execute-time 5000 \
            --source-name input --source-num-of-stream 10 \
            --source-write-period 0 \
            --source-max-records-per-stream 100000 \
            --sink-name output  \
            --debug-dataflow-activity-detail \
            --debug-dataflow-task \
            --debug-dataflow-vm \
            --dump-registry \
            --print-dataflow-info -1 \
            --junit-report /opt/junit-reports/KafkaIntegrationTest.xml"



#Run server failure
ssh -f -n -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master \ 
  "cd /opt/scribengin/scribengin && \
  ./bin/shell.sh dataflow-test random-server-failure --print-summary \
    --dataflow-name kafka-to-kafka \
    --failure-period 100000 --max-failure 10"


#Print the running processes
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "/opt/cluster/clusterCommander.py status"

sleep 30
#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/junit-reports/*.xml ./testresults/

#Clean up
$DOCKERSCRIBEDIR/../stopCluster.sh
