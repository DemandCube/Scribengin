#Set up docker images
DOCKERSCRIBEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
$DOCKERSCRIBEDIR/../docker.sh cluster --clean-containers --run-containers --deploy-scribengin --start-cluster

#make folder for test results
mkdir testresults

#Give everything time to come up
sleep 5

ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin info"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm info"

#ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "/opt/cluster/clusterCommander.py monitor --update-interval 15" &

#MONITOR_PID=$!

#Run start/stop/resume
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "mkdir -p /opt/junit-reports/ && \
      cd /opt/scribengin/scribengin && ./bin/shell.sh dataflow-test start-stop-resume  \
              --dataflow-id kafka-to-kafka-1 \
              --sleep-before-stop    30000 \
              --sleep-before-resume  15000 \
              --wait-before-start    15000 \
              --max-wait-for-stop    30000 \
              --max-wait-for-resume  30000 \
              --max-execution 25 \
              --print-summary \
              --junit-report /opt/junit-reports/DataflowTestStartStopResume.xml" &

FAILURE_PID=$!

#Run dataflow
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && \
          ./bin/shell.sh dataflow-test kafka-to-kafka \
             --dataflow-id    kafka-to-kafka-1 \
             --dataflow-name  kafka-to-kafka \
             --worker 3 \
             --executor-per-worker 2 \
             --duration 3600000 \
             --task-max-execute-time 10000 \
             --source-name input \
             --source-num-of-stream 10 \
             --source-write-period 0 \
             --source-max-records-per-stream 1500000 \
             --sink-name output \
             --print-dataflow-info -1 \
             --debug-dataflow-task \
             --debug-dataflow-vm \
             --debug-dataflow-activity \
             --junit-report /opt/junit-reports/KafkaIntegrationTest.xml \
             --dump-registry "

#Print the running processes
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "/opt/cluster/clusterCommander.py status"

#kill -9 $MONITOR_PID 
kill -9 $FAILURE_PID

#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/junit-reports/*.xml ./testresults/

#Clean up
$DOCKERSCRIBEDIR/.././docker.sh cluster --clean-containers
