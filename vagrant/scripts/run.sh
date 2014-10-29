#set -x

# trap keyboard interrupt (control-c)
trap control_c SIGINT

control_c()
# run if user hits control-c
{
  echo -en "\n*** Killing Scribengin ProcessID: $SCRIBE_PROC_ID ***\n"
  kill ${SCRIBE_PROC_ID}
  wait ${SCRIBE_PROC_ID}
}

usage()
{
  echo -e "Usage:\n./run.sh [topic1] [topic2] ... [topicN]"
}

#Make sure topics are passed in
if [ $# = 0 ]
  then
    usage
    exit
fi

#Adding this because there's an issue not getting what was set up in setup.sh
source ~/.bashrc

CWD="$(pwd)"

TOPICS=""
for X in $@
do
  TOPICS="$X,$TOPICS"
done
#Remove the final "," character
TOPICS=${TOPICS%?}


SCRIBEJAR="${CWD}/Scribengin/build/release/Scribengin/libs/scribengin-1.0-SNAPSHOT.jar"
#Remove old snapshot if its there
hdfs dfs -rm /scribengin-1.0-SNAPSHOT.jar || true
#Move new one to hdfs
hdfs dfs -copyFromLocal $SCRIBEJAR /


for X in $@
do
  echo "CREATING TOPIC: $X"
  ./bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 partition 1 --topic $X || true
done

cd $CWD/Scribengin
hdfs dfs -mkdir -p /user/yarn/ || true
hdfs dfs -chown yarn:yarn /user/yarn/ || true
java -cp $SCRIBEJAR com.neverwinterdp.scribengin.ScribeMaster --topic $TOPICS --broker_list 127.0.0.1:9092 --partition 0 --mode yarn --pre_commit_path_prefix /user/yarn/tmp --commit_path_prefix /user/yarn   --clean_start --hdfsPath hdfs://localhost/ &
#--hdfsPath hdfs://127.0.0.1:50010 --checkpoint_interval 5000 
SCRIBE_PROC_ID=$!
echo "Scribengin process ID: $SCRIBE_PROC_ID"

#Wait until somebody hits ctrl+c
wait
