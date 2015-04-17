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
                              ./bin/shell.sh dataflow-test start-stop-resume --sleep-before-execute 10000 \
                              --max-wait-for-stop  20000 --max-wait-for-resume  20000  --print-summary "

#Get results
#scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/dataflowStartStopResumeTest.xml ./testresults/

#Clean up
$DOCKERSCRIBEDIR/../stopCluster.sh
