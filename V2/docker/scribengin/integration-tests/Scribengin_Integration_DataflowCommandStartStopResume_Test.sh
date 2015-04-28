#Set up docker images
DOCKERSCRIBEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
$DOCKERSCRIBEDIR/../docker.sh cluster --clean-containers --run-containers --deploy-scribengin --start-cluster

#make folder for test results
mkdir testresults

#Give everything time to come up
sleep 5

ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin info"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm info"



#Run start/stop/resume
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "mkdir -p /opt/junit-reports/ && \
      cd /opt/scribengin/scribengin && nohup ./bin/shell.sh dataflow-test start-stop-resume  \
      --wait-before-start 5000 --sleep-before-execute 10000 --max-wait-for-stop  10000 \
      --max-wait-for-resume  10000  --print-summary  \
      --junit-report /opt/junit-reports/DataflowTestStartStopResume.xml" &

#Run dataflow
ssh  -o StrictHostKeyChecking=no neverwinterdp@hadoop-master \
   "cd /opt/scribengin/scribengin && \
   ./bin/shell.sh dataflow-test kafka-to-kakfa  --dataflow-name  kafka-to-kafka --worker 2 \
     --executor-per-worker 2 --duration 1800000 --task-max-execute-time 5000 \
     --source-name input --source-num-of-stream 10 --source-write-period 0  \
     --source-max-records-per-stream 1000000 --sink-name output --debug-dataflow-activity-detail \
     --debug-dataflow-task --dump-registry --print-dataflow-info -1 \
     --junit-report /opt/junit-reports/KafkaIntegrationTest.xml"

#Print the running processes
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "/opt/cluster/clusterCommander.py status"

wait

#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/junit-reports/*.xml ./testresults/

#Clean up
$DOCKERSCRIBEDIR/.././docker.sh cluster --clean-containers
