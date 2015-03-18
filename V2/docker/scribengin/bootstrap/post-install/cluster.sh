#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

USER="neverwinterdp"
APP_HOME=/opt

PRINT_INSTRUCTIONS=true

HADOOP_SERVERS="";
HADOOP_MASTER_SERVERS="";
HADOOP_WORKER_SERVERS="";
ZOOKEEPER_SERVERS="";
KAFKA_SERVERS="";
ALL_SERVERS="";

function has_opt() {
  OPT_NAME=$1
  shift
  #Par the parameters
  for i in "$@"; do
    if [[ $i == $OPT_NAME* ]] ; then
      echo "true"
      return
    fi
  done
  echo "false"
}

function get_opt() {
  OPT_NAME=$1
  DEFAULT_VALUE=$2
  shift
  #Parse the parameters
  for i in "$@"; do
    if [[ $i == $OPT_NAME* ]] ; then
      value="${i#*=}"
      echo "$value"
      return
    fi
  done
  echo $DEFAULT_VALUE
}

function h1() {
  echo ""
  echo "***********************************************************************************************************"
  echo "$@"
  echo "***********************************************************************************************************"
}

function h2() {
  echo ""
  echo "$@"
  #underline="${$@/.*/-}"
  #echo "$underline"
  echo $@ | sed -e 's/./\./g'
}

function inst() {
  if [[ $PRINT_INSTRUCTIONS ]]; then
    echo "/**                                                                                                                           "
    lines=()
    while read -r line; do
      lines+=("$line")
    done <<< "$@"

    lines_len=${#lines[@]}

    for(( i=0; i<${lines_len}; i++ ));
    do
      line=${lines[$i]} 
      #ignore the empty lines
      if [[ $line == "" ]] 
      then
        continue 
      fi

      progress=" *"

      for word in $line
      do
          progress="$progress $word"
          echo -ne "$progress\r"
          sleep 0.1
      done
      echo "$progress"
    done
    echo " **/"
  fi
}

#Parse /etc/hosts file to get the cluster hostname
function parse_hosts_file() {
  FILENAME="/etc/hosts"
  while read LINE
  do
    if [[ $LINE ==  $'## scribengin server ##'* ]] 
    then  
     BEGIN_SCRIBENGIN_SERVER=TRUE 
     continue
    elif [[ $LINE == \#* ]]  #Ignore the comment line
    then
      continue 
    elif [[ $LINE == "" ]]  #Ignore the empty line
    then
      continue 
    fi
    
    if [[ -n "$BEGIN_SCRIBENGIN_SERVER" ]] #Check not null, empty BEGIN_SCRIBENGIN_SERVER
    then 
      arrLine=(${LINE// / }) #split line that contains '$IP $HOSTNAME' format
      hostname=${arrLine[1]}

      if [[ $hostname ==  $'hadoop'* ]]
      then  
        HADOOP_SERVERS="$HADOOP_SERVERS $hostname"
        if [[ $hostname ==  $'hadoop-master'* ]]; then  
          HADOOP_MASTER_SERVERS="$HADOOP_MASTER_SERVERS $hostname"
        elif [[ $hostname ==  $'hadoop-worker'* ]]; then
          HADOOP_WORKER_SERVERS="$HADOOP_WORKER_SERVERS $hostname"
        fi
      elif [[ $hostname ==  $'zookeeper'* ]]
      then
        ZOOKEEPER_SERVERS="$ZOOKEEPER_SERVERS $hostname"
      elif [[ $hostname ==  $'kafka'* ]]
      then
        KAFKA_SERVERS="$KAFKA_SERVERS $hostname"
      fi
    fi
  done < $FILENAME
  
  ALL_SERVERS="$HADOOP_SERVERS $ZOOKEEPER_SERVERS $KAFKA_SERVERS"

}


function confirmYN() {
 while true; do
    read -p "$@" yn
    case $yn in
      [Yy]* ) break;;
      [Nn]* ) exit;;
      * ) echo "Please answer yes or no.";;
    esac
  done
}

function cluster_exec() {
  for server in $ALL_SERVERS; do
    h1 "Execute '$@' On  $server"
    ssh  $USER@$server "cd $bin && $@"
  done
}

function cluster_sync() {
  HOST=$(hostname)
  for server in $ALL_SERVERS; do
    if [[ $HOST =~ $server ]]; then  
      echo "Ignore $server" 
    else
      h1 "synchronized data with $server"
      rsync -a -r -c -P --delete --ignore-errors $APP_HOME/ $USER@$server:/opt
    fi
  done
}

function servers_exec() {
  servers=$1
  shift
  for server in $servers; do
    h2 "Execute '$@' On  $server"
    ssh  $USER@$server "cd $bin && $@"
  done
}

function get_javaagent_options(){
  JMXPORT=$1
  JVM_AGENT_OPTS="-javaagent:/opt/scribengin/jvmagent/libs/jvmagent.registry-1.0-SNAPSHOT.jar=/opt/scribengin/jvmagent/libs"
  JVM_AGENT_OPTS="$JVM_AGENT_OPTS -Dcom.sun.management.jmxremote"
  JVM_AGENT_OPTS="$JVM_AGENT_OPTS -Dcom.sun.management.jmxremote.port=$JMXPORT"
  JVM_AGENT_OPTS="$JVM_AGENT_OPTS -Dcom.sun.management.jmxremote.local.only=false"
  JVM_AGENT_OPTS="$JVM_AGENT_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
  JVM_AGENT_OPTS="$JVM_AGENT_OPTS -Dcom.sun.management.jmxremote.local.only=false"
  JVM_AGENT_OPTS="$JVM_AGENT_OPTS -Dcom.sun.management.jmxremote.ssl=false"
  echo $JVM_AGENT_OPTS
}

function hadoop_clean() {
  h1 "Clean hadoop data and logs"
  inst "this step will remove the data directory(/opt/hadoop/data) and the log directory(/opt/hadoop/logs)"
  servers_exec "$HADOOP_SERVERS" "rm -rf /opt/hadoop/data && rm -rf /opt/hadoop/logs"

  h1 "Reformat the dfs"
  inst "this step will run the command: /opt/hadoop/bin/hdfs namenode -format"
  servers_exec "$HADOOP_MASTER_SERVERS" "/opt/hadoop/bin/hdfs namenode -format"
}

function hadoop_start() {
  #Par the parameters
  clean=$(has_opt --clean $@)

  #clean the hadoop data and logs if clean = true
  if  $clean  ; then
    hadoop_clean
  fi

  servers_exec  "$HADOOP_MASTER_SERVERS" "/opt/hadoop/sbin/configure.sh"
  servers_exec  "$HADOOP_WORKER_SERVERS" "/opt/hadoop/sbin/configure.sh"
  
  h1 "Start hadoop dfs"
  servers_exec  "$HADOOP_MASTER_SERVERS" "/opt/hadoop/sbin/start-dfs.sh"

  h1 "Start hadoop yarn"
  servers_exec  "$HADOOP_MASTER_SERVERS" "/opt/hadoop/sbin/start-yarn.sh"
}

function hadoop_stop() {
  h1 "Stop hadoop yarn"
  servers_exec  "$HADOOP_MASTER_SERVERS" "/opt/hadoop/sbin/stop-yarn.sh"

  h1 "Stop hadoop dfs"
  servers_exec  "$HADOOP_MASTER_SERVERS" "/opt/hadoop/sbin/stop-dfs.sh"
}

function hadoop_log_grep() {
  h1 "Hadoop Logs"
  servers_exec "$HADOOP_SERVERS" "find  /opt/hadoop/logs -name '*.log' -exec grep $@ {} \; -print"
}

function hadoop_std_grep() {
  h1 "Hadoop Container stdout/stderr"
  servers_exec "$HADOOP_SERVERS" "find  /opt/hadoop/logs -name 'stdout' -exec grep $@ {} \; -print"
  servers_exec "$HADOOP_SERVERS" "find  /opt/hadoop/logs -name 'stderr' -exec grep $@ {} \; -print"
}

function hadoop_console_tail() {
  h1 "Hadoop Container stdout/stderr"
  servers_exec "$HADOOP_SERVERS" "find  /opt/hadoop/logs -name 'stdout' -exec tail $@ {} \; -print"
  servers_exec "$HADOOP_SERVERS" "find  /opt/hadoop/logs -name 'stderr' -exec tail $@ {} \; -print"
}

function hadoop_kill_data_node(){
  h1 "kill datanode on $@"
  servers_exec "$@" "pkill -9 -f datanode"
}

function hadoop_kill_yarn_node(){
  h1 "kill resourcemanager on $@"
  servers_exec "$@" "pkill -9 -f resourcemanager"
  h1 "kill nodemanager on $@"
  servers_exec "$@" "pkill -9 -f nodemanager"
}

function zookeeper_clean() {
  h1 "Clean zookeeper data and logs"
  inst $'This step will:\n
         1.  Remove the data directory(/opt/zookeeper/data) \n
         2.  Remove the log file(/opt/zookeeper/logs/zookeeper.out)'
  servers_exec "$ZOOKEEPER_SERVERS" "rm -rf /opt/zookeeper/data && rm -rf /opt/zookeeper/zookeeper.out && rm -rf /opt/zookeeper/logs"
}

function zookeeper_start() {
  #Parse the parameters
  clean=$(has_opt --clean $@)
  javaagent=$(has_opt --with-javaagent $@)
  JMXPORT=$(get_opt --jmxport 10001 $@)
  
  #clean the zookeeper data and logs if clean = true
  if  $clean  ; then
    zookeeper_clean
  fi

  if $javaagent ; then
    JVM_AGENT_OPTS=$(get_javaagent_options $JMXPORT)
  fi
  
  servers_exec  "$ZOOKEEPER_SERVERS" "/opt/zookeeper/bin/configure.sh"
  
  if $javaagent ; then
    h1 "Start zookeeper with javaagent enabled"
    servers_exec   "$ZOOKEEPER_SERVERS" "ZOO_LOG4J_PROP='INFO,ROLLINGFILE' ZOO_LOG_DIR=/opt/zookeeper/logs JVMFLAGS=\"$JVM_AGENT_OPTS\" /opt/zookeeper/bin/zkServer.sh start"
  else
    h1 "Start zookeeper"
    servers_exec  "$ZOOKEEPER_SERVERS" "ZOO_LOG4J_PROP='INFO,ROLLINGFILE' ZOO_LOG_DIR=/opt/zookeeper/logs  /opt/zookeeper/bin/zkServer.sh start"
  fi
  
}

function zookeeper_stop() {
  h1 "Stop the zookeeper servers"
  servers_exec  "$ZOOKEEPER_SERVERS" "/opt/zookeeper/bin/zkServer.sh stop"
}

function zookeeper_log_grep() {
  h1 "Zookeeper Logs"
  servers_exec "$ZOOKEEPER_SERVERS" "find  /opt/zookeeper/logs -name '*' -exec grep $@ {} \; -print"
}

function zookeeper_kill_node(){
  h1 "kill zookeeper on $@"
  servers_exec "$@" "pkill -9 -f QuorumPeerMain"
}


function kafka_clean() {
  h1 "Clean kafka data and logs"
  inst $'This step will: \n
         1.  Remove the data directory /opt/kafka/data \n
         2.  Remove the log file /opt/kafka/logs'
  servers_exec "$KAFKA_SERVERS" "rm -rf /opt/kafka/data && rm -rf /opt/kafka/logs"
}

function kafka_start() {
  #Parse the parameters
  clean=$(has_opt --clean $@)
  javaagent=$(has_opt --with-javaagent $@)
  withzk=$(has_opt --with-zookeeper $@)
  config_path=$(get_opt --config-path '/opt/kafka/config/default.properties' $@)

  if $withzk ; then
    zookeeper_start $@
  fi

  #clean the kafka data and logs if clean = true
  if  $clean  ; then
    kafka_clean
  fi

  if $javaagent ; then
    JVM_AGENT_OPTS=$(get_javaagent_options)
  fi

  if $javaagent ; then
    h1 "Start kafka with javaagent enabled"
    servers_exec  "$KAFKA_SERVERS" "/opt/kafka/bin/configure.sh $config_path"
    servers_exec  "$KAFKA_SERVERS" "KAFKA_JMX_OPTS=\"$JVM_AGENT_OPTS\" /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties"
  else
    h1 "Start kafka"
    servers_exec  "$KAFKA_SERVERS" "/opt/kafka/bin/configure.sh $config_path"
    servers_exec  "$KAFKA_SERVERS" "/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties"
  fi
}

function kafka_stop() {
  h1 "Stop the kafka servers"
  servers_exec  "$KAFKA_SERVERS" "/opt/kafka/bin/kafka-server-stop.sh"
}

function kafka_log_grep() {
  h1 "kafka Logs"
  servers_exec "$KAFKA_SERVERS" "find  /opt/kafka/logs -name '*.log' -exec grep $@ {} \; -print"
}

function kafka_kill_node(){
  h1 "kill kafka on $@"
  servers_exec "$@" "pkill -9 -f kafka"
}

parse_hosts_file 

# get sub command
COMMAND=$1
shift


echo ""
echo "************************************************************************************************************"
echo "Server hadoop role   : $HADOOP_SERVERS"
echo "Server zookeeper role: $ZOOKEEPER_SERVERS"
echo "Server kafka role    : $KAFKA_SERVERS"
echo "All Server           : $ALL_SERVERS"
echo "************************************************************************************************************"
echo ""

if [ "$COMMAND" = "exec" ] ; then
  cluster_exec $@
elif [ "$COMMAND" = "sync" ] ; then
  confirmYN "Do you want to sync this program with the other members(Y/N)?"
  cluster_sync
elif [ "$COMMAND" = "start" ] ; then
  zookeeper_start $@
  withKafka=false
  for i in "$@"; do
    case $i in
      --with-kafka)
      withKafka=true
      ;;
      #unknown option
    esac
  done
  if  $withKafka  ; then
    kafka_start $@
  fi
  hadoop_start $@
elif [ "$COMMAND" = "stop" ] ; then
  hadoop_stop $@
  kafka_stop $@
  zookeeper_stop $@
elif [ "$COMMAND" = "clean" ] ; then
  hadoop_clean $@
  kafka_clean $@
  zookeeper_clean $@
elif [ "$COMMAND" = "log-grep" ] ; then
  hadoop_log_grep $@
  kafka_log_grep $@
  zookeeper_log_grep $@
elif [ "$COMMAND" = "zookeeper" ] ; then
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "start" ] ; then
    zookeeper_start $@
  elif [ "$SUB_COMMAND" = "stop" ] ; then
    zookeeper_stop $@
  elif [ "$SUB_COMMAND" = "clean" ] ; then
    zookeeper_clean
  elif [ "$SUB_COMMAND" = "log-grep" ] ; then
    zookeeper_log_grep $@
  elif [ "$SUB_COMMAND" = "kill-node" ] ; then
    zookeeper_kill_node $@
  fi
elif [ "$COMMAND" = "kafka" ] ; then
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "start" ] ; then
    kafka_start $@
  elif [ "$SUB_COMMAND" = "stop" ] ; then
    kafka_stop $@
  elif [ "$SUB_COMMAND" = "clean" ] ; then
    kafka_clean
  elif [ "$SUB_COMMAND" = "log-grep" ] ; then
    kafka_log_grep $@
  elif [ "$SUB_COMMAND" = "kill-node" ] ; then
    kafka_kill_node $@
  fi
elif [ "$COMMAND" = "hadoop" ] ; then
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "start" ] ; then
    hadoop_start $@
  elif [ "$SUB_COMMAND" = "stop" ] ; then
    hadoop_stop $@
  elif [ "$SUB_COMMAND" = "clean" ] ; then
    hadoop_clean
  elif [ "$SUB_COMMAND" = "log-grep" ] ; then
    hadoop_log_grep $@
  elif [ "$SUB_COMMAND" = "std-grep" ] ; then
    hadoop_std_grep $@
  elif [ "$SUB_COMMAND" = "console-tail" ] ; then
    hadoop_console_tail $@
  elif [ "$SUB_COMMAND" = "kill-data-node" ] ; then
    hadoop_kill_data_node $@
  elif [ "$SUB_COMMAND" = "kill-yarn-node" ] ; then
    hadoop_kill_yarn_node $@
  fi
elif [ "$COMMAND" = "test" ] ; then
  echo "test"
else
  echo "Usage: ./cluster.sh [command] [subcommand] [options]"
  echo "cluster command options: "
  echo "  exec                             : To execute the shell command on all the servers or a group of servers"
  echo "  sync                             : To copy this program to the fetcher members"
  echo "  zookeeper [start|stop]           : Start/stop ZK on all ZK servers."
  echo "  zookeeper start --with-javaagent : Start ZK on all ZK servers, launch JMX on port 10001."
  echo "  kafka [start|stop]               : Start/stop Kafka on all Kafka servers."
  echo "  kafka start --with-javaagent     : Start Kafka on all Kafka servers, launch JMX on port 10001."
  echo "  kafka start --with-zookeeper     : Start Kafka on all Kafka servers, launch ZooKeeper on all ZooKeeper servers."
fi
