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
ssh -f -n -o StrictHostKeyChecking=no neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && \
                              nohup ./bin/shell.sh dataflow-test kafka-to-kakfa  --dataflow-name  kafka-to-kafka --worker 2 \
                              --executor-per-worker 2 --duration 300000 --task-max-execute-time 5000 \
                              --source-name input --source-num-of-stream 10 --source-write-period 0  \
                              --source-max-records-per-stream 100000 --sink-name output --debug-dataflow-activity-detail \
                              --debug-dataflow-task --dump-registry --print-dataflow-info -1"

sleep 10

#Run start/stop/resume
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && \
                              ./bin/shell.sh dataflow-test start-stop-resume --sleep-before-execute 10000 \
                              --max-wait-for-stop  20000 --max-wait-for-resume  20000  --print-summary "



#Get results
#scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/dataflowStartStopResumeTest.xml ./testresults/

#Clean up
$DOCKERSCRIBEDIR/../stopCluster.sh
