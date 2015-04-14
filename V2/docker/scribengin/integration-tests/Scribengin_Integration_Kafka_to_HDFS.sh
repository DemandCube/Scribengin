#Set up Scribengin cluster
cd V2/
./installDependencies.sh
cd docker/scribengin/
./docker.sh container clean || true
./docker.sh image clean || true
./scribengin.sh build
./docker.sh image build
./docker.sh container run
sudo ./docker.sh update-hosts

#Start cluster
ssh -o StrictHostKeyChecking=no neverwinterdp@hadoop-master "cd /opt/cluster && ./setup.sh && python clusterCommander.py zookeeper --clean --start kafka --clean start"

sleep 20
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm start"
sleep 5
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin start"
sleep 5
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin info"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm info"


#Run dataflow
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh  dataflow-test kafka-hdfs --worker 3 --executor-per-worker 1 --duration 70000 --task-max-execute-time 1000 --source-name input --source-num-of-stream 10 --source-write-period 5 --source-max-records-per-stream 3000 --sink-name output --junit-report Kafka_to_HDFS_IntegrationTest.xml"

#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/Kafka_to_HDFS_IntegrationTest.xml ./

#Clean up
./docker.sh container clean || true
./docker.sh image clean || true
