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
        elif [[ $hostname ==  $'zookeeper'* ]]; then
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
  clean=false
  mode="default"
  for i in "$@"; do
    case $i in
      -c|--clean)
      clean=true 
      ;;
      -m=*|--mode=*)
      mode="${i#*=}"
      ;;
         #unknown option
    esac
  done

  #clean the hadoop data and logs if clean = true
  if  $clean  ; then
    hadoop_clean
  fi

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

function zookeeper_clean() {
  h1 "Clean zookeeper data and logs"
  inst $'This step will:\n
         1.  Remove the data directory(/opt/zookeeper/data) \n
         2.  Remove the log file(/opt/zookeeper/logs/zookeeper.out)'
  servers_exec "$ZOOKEEPER_SERVERS" "rm -rf /opt/zookeeper/data && rm -rf /opt/zookeeper/zookeeper.out"
}

function zookeeper_start() {
  #Par the parameters
  clean=false
  for i in "$@"; do
    case $i in
      -c|--clean)
      clean=true 
      ;;
      #unknown option
    esac
  done

  #clean the hadoop data and logs if clean = true
  if  $clean  ; then
    zookeeper_clean
  fi

  h1 "Start zookeeper"
  servers_exec  "$ZOOKEEPER_SERVERS" "/opt/zookeeper/bin/zkServer.sh start"
}

function zookeeper_stop() {
  h1 "Stop the zookeeper servers"
  servers_exec  "$ZOOKEEPER_SERVERS" "/opt/zookeeper/bin/zkServer.sh stop"
}

function kafka_clean() {
  h1 "Clean kafka data and logs"
  inst $'This step will: \n
         1.  Remove the data directory /opt/kafka/data \n
         2.  Remove the log file /opt/kafka/logs'
  servers_exec "$KAFKA_SERVERS" "rm -rf /opt/kafka/data && rm -rf /opt/kafka/logs"
}

function kafka_start() {
  #Par the parameters
  clean=false
  for i in "$@"; do
    case $i in
      -c|--clean)
      clean=true 
      ;;
      #unknown option
    esac
  done

  #clean the hadoop data and logs if clean = true
  if  $clean  ; then
    kafka_clean
  fi

  h1 "Start kafka"
  servers_exec  "$KAFKA_SERVERS" "/opt/kafka/bin/configure.sh"
  servers_exec  "$KAFKA_SERVERS" "/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties"
}

function kafka_stop() {
  h1 "Stop the kafka servers"
  servers_exec  "$KAFKA_SERVERS" "/opt/kafka/bin/kafka-server-stop.sh"
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
  kafka_start $@
  hadoop_start $@
elif [ "$COMMAND" = "stop" ] ; then
  hadoop_stop $@
  kafka_stop $@
  zookeeper_stop $@
elif [ "$COMMAND" = "clean" ] ; then
  hadoop_clean $@
  kafka_clean $@
  zookeeper_clean $@
elif [ "$COMMAND" = "zookeeper" ] ; then
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "start" ] ; then
    zookeeper_start $@
  elif [ "$SUB_COMMAND" = "stop" ] ; then
    zookeeper_stop $@
  elif [ "$SUB_COMMAND" = "clean" ] ; then
    zookeeper_clean
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
  fi
else
  echo "cluster command options: "
  echo "  exec                  : To execute the shell command on all the servers or a group of servers"
  echo "  sync                  : To copy this program to the fetcher members"
fi
