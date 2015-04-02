df -ah

#Set up Scribengin cluster
cd V2/
./installDependencies.sh
cd docker/scribengin/
./docker.sh container clean || true
./docker.sh image clean || true
./scribengin.sh build
./docker.sh image build
./docker.sh container run --zk-server=2 --kafka-server=4
sudo ./docker.sh update-hosts

#Start cluster
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt && ./cluster.sh start --with-kafka --clean"

#Give everything time to come up
sleep 20


#Run failure simulator in the background
ssh -f -n -o StrictHostKeyChecking=no neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin/tools/kafka/  && nohup ./failure_simulator.sh simulate --kafka-broker=kafka-1,kafka-2,kafka-3 --wait-before-start=10 --kafka-failure=10 --min-kafka=2"

#Run kafkaStabilityCheckTool
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin/tools/kafka/ && ./kafka.sh test stability --zk-connect zookeeper-1:2181 --topic stabilitytest --replication 3 --send-period 0 --send-writer-type ack --send-max-duration 1800000 --send-max-per-partition 20000000 --producer:message.send.max.retries=5 --producer:retry.backoff.ms=100 --producer:queue.buffering.max.ms=1000 --producer:queue.buffering.max.messages=15000 --producer:topic.metadata.refresh.interval.ms=-1 --producer:batch.num.messages=100 --producer:acks=all --consume-max 100000000 --consume-max-duration 6000000 --junit-report KafkaMessageCheckTool.xml --producer:compression.type=gzip"

#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/tools/kafka/KafkaMessageCheckTool.xml ./

#Allow failure simulator to save test results
sleep 10

#Get results for failure simulatore
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/tools/kafka/results/FAILURE_TEST_failure_simulator.xml ./

df -ah

#Clean up
./docker.sh container clean || true
./docker.sh image clean || true
