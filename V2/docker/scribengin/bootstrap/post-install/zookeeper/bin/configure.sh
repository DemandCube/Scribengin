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

HOST_ID="${HOSTNAME/zookeeper-/}"

source $bin/../conf/zoo.cfg
echo "Config for the username: $dataDir"
if [ ! -f "$dataDir" ]; then
       mkdir -p "$dataDir" 2>/dev/null
   fi
echo "$HOST_ID" > $dataDir/myid

ZOOKEEPER_CONNECT=""
for server in $ZOOKEEPER_SERVERS
do
  current_host_id="${server/zookeeper-/}"
  if grep -w -q "server.$current_host_id=*" $bin/../conf/zoo.cfg; then
    sed -i -e "s/server\.$current_host_id=.*/server\.$current_host_id=$server:2888:3888/g" $bin/../conf/zoo.cfg
  else
    echo "server.$current_host_id=$server:2888:3888" >> $bin/../conf/zoo.cfg
  fi
done

