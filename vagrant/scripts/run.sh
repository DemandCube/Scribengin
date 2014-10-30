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
  echo -e "Usage:\n./run.sh [options] [topic1] [topic2] ... [topicN]"
  echo -e "    -k [host:port]  = Host/port for Kafka. Default is [hostname]:9092"
  echo -e "    -h [host:port]  = Host/port for HDFS. Default is hdfs://[hostname]:8020"
  echo -e "    -c              = Enable clean start algorithm for ScribeConsumer. Default is disabled"
}


#Adding this because there's an issue not getting what was set up in setup.sh
source ~/.bashrc
#Our working directory
CWD=$(pwd)
#Get this machine's hostname
HOSTNAME=$(hostname)


KAFKA="$HOSTNAME:9092"
CLEANSTART=false
HDFSPATH="hdfs://$HOSTNAME:8020"
while getopts ":k:h:c" Option
    # Initial declaration.
    # a, b, c, d, e, f, and g are the flags expected.
    # The : after flag 'e' shows it will have an option passed with it.
    do
      case $Option in
        k ) KAFKA=$OPTARG ;;
        c ) CLEANSTART=true ;;
        h ) HDFSPATH=$OPTARG ;;
        * ) usage; exit -1  ;;
        
  esac
done

shift $(( OPTIND - 1 ))


#Make sure topics are passed in
if [ $# = 0 ]
  then
    usage
    exit
fi


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

cd $CWD/kafka_2.8.0-0.8.0
for X in $@
do
  echo "CREATING TOPIC: $X"
  ./bin/kafka-create-topic.sh --zookeeper localhost:2181 --replica 1 partition 1 --topic $X || true
done

cd $CWD/Scribengin
hdfs dfs -mkdir -p /user/yarn/ || true
hdfs dfs -chown yarn:yarn /user/yarn/ || true

CMD="java -cp $SCRIBEJAR com.neverwinterdp.scribengin.ScribeMaster --topic $TOPICS --broker_list $KAFKA --partition 0  --pre_commit_path_prefix /user/yarn/tmp --commit_path_prefix /user/yarn --hdfsPath $HDFSPATH --mode yarn "
if [ $CLEANSTART = true ] ; then
    CMD="$CMD --clean_start &"
else
    CMD="$CMD &"
fi
echo "$CMD"
eval $CMD
SCRIBE_PROC_ID=$!
echo "Scribengin process ID: $SCRIBE_PROC_ID"

#Wait until somebody hits ctrl+c
wait
