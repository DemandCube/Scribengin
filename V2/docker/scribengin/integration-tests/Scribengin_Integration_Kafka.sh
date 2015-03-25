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
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh  dataflow-test kafka --worker 3 --executor-per-worker 1 --duration 70000 --task-max-execute-time 1000 --source-name input --source-num-of-stream 10 --source-write-period 5 --source-max-records-per-stream 3000 --sink-name output"

#Clean up
./docker.sh container clean || true
./docker.sh image clean || true
