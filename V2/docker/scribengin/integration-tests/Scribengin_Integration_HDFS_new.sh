#Set up Scribengin cluster
CWD=`pwd`
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR
../startCluster.sh

#Start cluster
ssh -o StrictHostKeyChecking=no neverwinterdp@hadoop-master "cd /opt/cluster && python clusterCommander.py cluster --start --clean status"

sleep 5
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin info"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm info"

#Run dataflow
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh dataflow-test hdfs --worker 3 --executor-per-worker 1 --duration 10000 --task-max-execute-time 1000 --source-num-of-stream 10 --source-max-records-per-stream 1000 --source-dataflowName hello-source --sink-dataflowName hello-source --junit-report HDFS_IntegrationTest.xml"

#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/HDFS_IntegrationTest.xml ./

#Clean up
../stopCluster.sh
cd $CWD
