#set -x

CWD="$(pwd)"

#This is to avoid permission problems
echo "export HADOOP_USER_NAME=hdfs" >> ~/.bashrc
#This is to set the native hadoop library path for java to find
echo "export LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native/:$LD_LIBRARY_PATH" >> ~/.bashrc
source ~/.bashrc

#Install java and wget
sudo yum install java-1.7.0-openjdk java-1.7.0-openjdk-devel wget -y

#Download our Ambari setup script
git clone git://github.com/DemandCube/hadoop-single-node-cluster
cd hadoop-single-node-cluster
sudo ./INSTALL-HADOOP -y


#Checkout the whole project, set up gradlew
cd $CWD
git clone git://github.com/DemandCube/NeverwinterDP
cd NeverwinterDP
./neverwinterdp.sh checkout
./gradlew

cd $CWD/NeverwinterDP-Commons/
./gradlew clean build install release -x test

cd $CWD/Queuengin/
./gradlew clean build install release -x test

cd $CWD/Scribengin
./gradlew clean build install release -x test


#Download kafka
cd $CWD
wget https://archive.apache.org/dist/kafka/0.8.0/kafka_2.8.0-0.8.0.tar.gz
tar -xzf kafka_2.8.0-0.8.0.tar.gz

#Launch kafka
cd kafka_2.8.0-0.8.0
#Launch Kafka at system startup
echo $CWD/kafka_2.8.0-0.8.0/bin/kafka-server-start.sh $CWD/kafka_2.8.0-0.8.0/config/server.properties >> /etc/rc.d/rc.local
#Launch Kafka now
./bin/kafka-server-start.sh ./config/server.properties &
KAFKA_PROC_ID=$!
echo "Kafka process ID: $KAFKA_PROC_ID"