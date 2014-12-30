#!/usr/bin/env bash

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

USER="neverwinterdp"
APP_HOME=/opt

HADOOP_SERVERS="";
ZOOKEEPER_SERVERS="";
KAFKA_SERVERS="";
ALL_SERVERS="";

#Parse /etc/hosts file to get the cluster hostname
function parseHostsFile() {
  FILENAME="/etc/hosts"
  while read LINE
  do
    if [[ $LINE ==  $'## scribengin server ##'* ]] 
    then  
     BEGIN_SCRIBENGIN_SERVER=TRUE 
     continue
    elif [[ $LINE == \#* ]]   #Ignore the comment line
    then  
      continue 
    elif [[ $LINE == "" ]]    #Ignore the empty line
    then  
      continue 
    fi
    
    if [[ -n "$BEGIN_SCRIBENGIN_SERVER" ]] #Check not null, empty BEGIN_SCRIBENGIN_SERVER
    then
      arrLine=(${LINE// /}) #split line that contains '$IP $HOSTNAME' format
      hostname=${arrLine[1]}

      if [[ $hostname ==  $'hadoop'* ]] 
      then  
       HADOOP_SERVERS="$HADOOP_SERVERS $hostname"
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

function printHeader() {
  echo ""
  echo "###########################################################################################################"
  echo "$@"
  echo "###########################################################################################################"
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
    printHeader "Execute '$@' On  $server"
    ssh $USER@$server "cd $bin && $@"
  done
}

function cluster_sync() {
  for server in $ALL_SERVERS; do
    printHeader "synchronized data with $server"
    rsync -r -c -P --delete $APP_HOME $USER@$server:/opt/
  done
}

parseHostsFile 

# get sub command
COMMAND=$1
shift


echo ""
echo "************************************************************************************************************"
echo "Server hadoop role   : $GENERIC_SERVERS"
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
else
  echo "cluster command options: "
  echo "  exec                  : To execute the shell command on all the servers or a group of servers"
  echo "  sync                  : To copy this program to the fetcher members"
fi
