#Set up Scribengin cluster
cd V2/
./installDependencies.sh
cd docker/scribengin/
./docker.sh container clean || true
./docker.sh image clean || true
./scribengin.sh build
./docker.sh image build
./docker.sh container run --zk-server=2 --kafka-server=4
if [[ $OSTYPE == *"darwin"* ]] ; then
  ./docker.sh ip-route
  ./docker.sh update-hosts
else
  sudo ./docker.sh update-hosts
fi

#Start cluster
ssh -o StrictHostKeyChecking=no neverwinterdp@hadoop-master "cd /opt/cluster && ./setup.sh && python clusterCommander.py zookeeper --clean --start kafka --clean start"

#Give everything time to come up
sleep 20


#Run failure simulator in the background
ssh -o StrictHostKeyChecking=no neverwinterdp@hadoop-master "cd /opt/cluster && mkdir /opt/scribengin/scribengin/tools/kafka/build/"
ssh -f -n -o StrictHostKeyChecking=no neverwinterdp@hadoop-master "cd /opt/cluster && nohup                                  \
                                          python clusterCommander.py --debug                                                 \
                                           kafkafailure --servers kafka-1,kafka-2,kafka-3,kafka-4                            \
                                          --wait-before-start 30 --failure-interval 30 --kill-method restart                 \
                                          --servers-to-fail-simultaneously 1                                                 \
                                          --junit-report /opt/scribengin/scribengin/tools/kafka/build/kafkaFailureReport.xml \
                                          zookeeperfailure --servers zookeeper-1,zookeeper-2                                 \
                                          --wait-before-start 30 --failure-interval 30 --kill-method restart                 \
                                          --servers-to-fail-simultaneously 1                                                 \
                                          --junit-report /opt/scribengin/scribengin/tools/kafka/build/zkFailureReport.xml    \
                                          monitor --update-interval 10"


#Run kafkaStabilityCheckTool
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin/tools/kafka/ &&      \
                                          ./kafka.sh test stability --zk-connect zookeeper-1:2181                 \
                                          --topic stabilitytest --replication 2 --send-period 0                   \
                                          --send-writer-type ack --send-max-duration 1800000                      \
                                          --send-max-per-partition 10000000 --producer:message.send.max.retries=5 \
                                          --producer:retry.backoff.ms=100 --producer:queue.buffering.max.ms=1000  \
                                          --producer:queue.buffering.max.messages=15000                           \
                                          --producer:topic.metadata.refresh.interval.ms=-1                        \
                                          --producer:batch.num.messages=100 --producer:acks=all                   \
                                          --consume-max 50000000 --consume-max-duration 5550000                   \
                                          --junit-report build/KafkaMessageCheckTool.xml"

#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/tools/kafka/build/*.xml ./

#Allow failure simulator to save test results
#sleep 10

#Get results for failure simulator
#scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/tools/kafka/results/FAILURE_TEST_failure_simulator.xml ./

#Clean up
./docker.sh container clean || true
./docker.sh image clean || true
