#!/bin/bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

HOSTNAME=$(hostname)


HADOOP_SERVERS="";
HADOOP_MASTER_SERVERS="";
HADOOP_WORKER_SERVERS="";
ZOOKEEPER_SERVERS="";
KAFKA_SERVERS="";
ALL_SERVERS="";

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

parse_hosts_file 

HOST_ID="${HOSTNAME/kafka-/}"

ZOOKEEPER_CONNECT=""
for server in $ZOOKEEPER_SERVERS
do
  if [[ $ZOOKEEPER_CONNECT != "" ]]; then
    ZOOKEEPER_CONNECT+=','
  fi
  ZOOKEEPER_CONNECT+="$server:2181"
done

KAFKA_CONNECT=""
for server in $KAFKA_SERVERS
do
  if [[ $KAFKA_CONNECT != "" ]]; then
    KAFKA_CONNECT+=','
  fi
  KAFKA_CONNECT+="$server:9092"
done

CONFIG_NAME=$(get_opt --config-name default $@)

sed -i -e "s/broker\.id=.*/broker\.id=$HOST_ID/g"  $bin/../config/$CONFIG_NAME\.properties
sed -i -e "s/zookeeper\.connect=.*/zookeeper\.connect=$ZOOKEEPER_CONNECT/g"  $bin/../config/$CONFIG_NAME\.properties
