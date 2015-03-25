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
######ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt && ./cluster.sh kafka start --with-zookeeper"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt && ./cluster.sh start --with-kafka --clean"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm start"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin start"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin info"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm info"

#Give everything time to come up
sleep 20



#Run dataflow
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh dataflow hdfs --create-source"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh dataflow submit --deploy ../dataflows/hdfs --descriptor ../dataflows/hdfs/dataflow.json"

#Clean up
./docker.sh container clean || true
./docker.sh image clean || true
