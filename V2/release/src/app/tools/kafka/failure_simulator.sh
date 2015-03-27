#!/bin/bash
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

USER="neverwinterdp"

#### Include shell2junit library
. $bin/sh2ju.sh

PRINT_INSTRUCTIONS=true

function print_usage() {
  h1 "Usage"
  echo "kafka                           :Main command to invoke kafka related operations"
  echo "  restart                       :To restart any kafka server"
  echo "    --broker                    :Kafka brokers, Multiple values can be given in comma seperated value ( Example: --broker=kafka-1,kafka-2 )"
  echo "    --wait-before-start         :Time to wait before start kafka server, Measured in seconds ( Example: --wait-before-start=10 )"
  echo "    --clean			:Clean kafka old datas"
  echo "zookeeper                       :Main command to invoke zookeeper related operations"
  echo "  restart                       :To restart any zookeeper server"
  echo "    --zk-server                 :Zokeeper servers, Multiple values can be given in comma seperated value ( Example: --zk-server=zookeeper-1,zookeeper-2 )"
  echo "    --wait-before-start         :Time to wait before start zookeeper server, Measured in seconds ( Example: --wait-before-start=10 )"
  echo "    --clean                     :Clean zookeeper old datas"
  echo "simulate			:To simulate failures for kafka and zookeeper servers"
  echo "  --zk-failure                  :Time interval to fail zookeeper server ( Example: zk-failure=180 )"
  echo "  --kafka-failure               :Time interval to fail kafka broker ( Example: kafka-failure=60 )"
  echo "  --wait-before-start           :Time to wait before start kafka or zookeeper, Measured in seconds ( Example: wait-before-start=10 )"  
  echo "  --zk-server                   :Zokeeper servers, Multiple values can be given in comma seperated value ( Example: zk-server=zookeeper-1,zookeeper-2 )"
  echo "  --kafka-broker                :Kafka brokers, Multiple values can be given in comma seperated value ( Example: kafka-broker=kafka-1,kafka-2 )"
  echo "  --min-zk			:The minimum number of ZK nodes that must always stay up. (This is optional, by default it will keep 1 Zk node always alive)"
  echo "  --min-kafka                   :The minimum number of kafka brokers that must always stay up. (This is optional, by default it will keep 1 kafka broker always alive)"
  echo " "
}

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
  
  #Par the parameters
  for i in "$@"; do
    index=$(($index+1))
    if [[ $i == $OPT_NAME* ]] ; then
      value="${i#*=}"
      echo "$value"
      return
    fi
  done
  echo $DEFAULT_VALUE
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

#Parse /etc/hosts file to get the cluster hostname
function parse_hosts_file() {
  FILENAME="/etc/hosts"
  while read LINE
  do
    if [[ $LINE == \#* ]]  #Ignore the comment line
    then
      continue 
    elif [[ $LINE == "" ]]  #Ignore the empty line
    then
      continue 
    fi
    
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
  done < $FILENAME
  
  ALL_SERVERS="$HADOOP_SERVERS $ZOOKEEPER_SERVERS $KAFKA_SERVERS"

}

function servers_exec() {
  servers=$1
  servers=`echo "$servers" | tr ',' ' '`
  shift
  for server in $servers; do
    h2 "Execute '$@' On  $server"
    ssh  $USER@$server "cd $bin && $@"
  done
}

function testProcess() {
  servers=$1
  process=$2
  operation=$3

  servers=`echo "$servers" | tr ',' ' '`
  if [[ "$process" == "zookeeper" ]]; then
    grep_process="QuorumPeerMain"
  elif [[ "$process" == "kafka" ]]; then
    grep_process="Kafka"
  fi

  for server in $servers; do
    retry=6
    count=1    
    while [ "$count" -le "$retry"  ]
    do
      sleep 5
      echo "Testing $process is running on $server - Try $count"
      pid=$(ssh -o "StrictHostKeyChecking no" $USER@$server "ps ax | grep -i '$grep_process' | grep java | grep -v grep | awk '{print \$1}'")

      echo "Running $grep_process-$pid on $server"
  
      if [[ "$operation" == "start" ]]; then
        if [[ -z "$pid" ]]; then
          if [[ "$count" -eq "$retry" ]]; then
            #### Failure
            success_command='-failed-to-start'
            juLog -name=$server$success_command false
            break
          fi
        else
          #### Success
          failure_command='-started-successfully'
          juLog -name=$server$failure_command true
          break
        fi
      elif [[ "$operation" == "stop" ]]; then
        if [[ -z "$pid" ]]; then
          #### Success
          success_command='-stopped-successfully'
          juLog -name=$server$success_command true
          break
        else
          if [[ "$count" -eq "$retry" ]]; then
            #### Failure
            failure_command="-failed-to-stop"
            juLog -name=$server$failure_command false
            break
          fi
        fi
      fi
      count=$[$count+1]
    done
  done
}

function clean_shutdown() {
  servers=$1
  commands=$2
  servers_exec "$servers" "$commands"
}

function kafka_start() {
  h1 "Start kafka server(s) $1"
  servers_exec  "$1" "/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties"
}

function kafka_stop() {
  h1 "Stop kafka server(s) $1"
  clean_shutdown "$1" "/opt/kafka/bin/kafka-server-stop.sh"
}

function kafka_clean() {
  h1 "Clean kafka data and logs"
  inst $'This step will: \n
         1.  Remove the data directory /opt/kafka/data \n
         2.  Remove the log file /opt/kafka/logs'
  servers_exec "$@" "rm -rf /opt/kafka/data && rm -rf /opt/kafka/logs"
}

function kafka_restart() {
  broker=$(get_opt --broker 'kafka-1' $@)
  wait_before_start=$(get_opt --wait-before-start 10 $@)
  clean=$(has_opt --clean $@)
 
  kafka_stop  "$broker" 

  #test kafka brokers are stoped
  testProcess $broker 'kafka' 'stop'

  h1 "Waiting for $wait_before_start to start failed kafka servers"  
  sleep $wait_before_start
  
  if  $clean  ; then 
    kafka_clean $@
  fi
  
  kafka_start "$broker"
  
  #test kafka brokers sre started
  testProcess $broker 'kafka' 'start' 
}

function zookeeper_clean() {
  h1 "Clean zookeeper data and logs"
  inst $'This step will:\n
         1.  Remove the data directory(/opt/zookeeper/data) \n
         2.  Remove the log file(/opt/zookeeper/logs/zookeeper.out)'
  servers_exec "$@" "rm -rf /opt/zookeeper/data && rm -rf /opt/zookeeper/zookeeper.out && rm -rf /opt/zookeeper/logs"
}

function zookeeper_start() {
  h1 "Start the zookeeper server $1"
  servers_exec "$1" "ZOO_LOG4J_PROP='INFO,ROLLINGFILE' ZOO_LOG_DIR=/opt/zookeeper/logs  /opt/zookeeper/bin/zkServer.sh start"
}

function zookeeper_stop() {
  h1 "Stop the zookeeper server $1"
  clean_shutdown "$1" "/opt/zookeeper/bin/zkServer.sh stop"
}

function zookeeper_restart() {
  zk_server=$(get_opt --zk-server 'zookeeper-1' $@)
  wait_before_start=$(get_opt --wait-before-start 10 $@)
  clean=$(has_opt --clean $@)
  
  zookeeper_stop "$zk_server"
  
  #test zookeeper stopped
  testProcess $zk_server 'zookeeper' 'stop'
  
  h1 "Waiting for $wait_before_start to start failed zookeeper servers"
  sleep $wait_before_start
  
  if  $clean  ; then
    zookeeper_clean $@
  fi
  
  zookeeper_start "$zk_server"

  #test zookeeper started
  testProcess $zk_server 'zookeeper' 'start'
}

function select_random_servers() {
  count=0
  IFS=',' read -ra SERVERS <<< "$1"
  random_num_of_server_to_die=$(shuf -i 1-${#SERVERS[@]} -n 1)
  actual_max_num_of_server_to_die=$2
  
  SELECTED_SERVERS=""
  for (( c=1; c<=$random_num_of_server_to_die; c++ ))
  do
   i=$(shuf -i 1-${#SERVERS[@]} -n 1)
   i=`expr $i - 1`
   s=${SERVERS[$i]}
   SELECTED_SERVERS="$SELECTED_SERVERS,$s"
   unset SERVERS[$i]
   SERVERS=("${SERVERS[@]}")
   if [ "$actual_max_num_of_server_to_die" -eq "$c" ] ; then
     break
   fi
  done

  echo "$SELECTED_SERVERS"
}

function kafka_failure_simulator() {
  temp_kafka_time=0
  kafka_failure_time=$(get_opt --kafka-failure 60 $@)
  wait_before_start=$(get_opt --wait-before-start 10 $@)
  brokers_to_kill=$(get_opt --kafka-broker '' $@)
  min_kafka=$(get_opt --min-kafka 1 $@)
  KAFKA_SERVERS_ARRAY=($KAFKA_SERVERS)
  actual_max_num_of_server_to_die=`expr ${#KAFKA_SERVERS_ARRAY[@]} - $min_kafka`

  if [ "$actual_max_num_of_server_to_die" -gt 0 ] ; then
    h1 "Kafka fail in $kafka_failure_time"

    while true;
    do
      sleep 1
      (( temp_kafka_time++ ))
      if [ "$kafka_failure_time" -eq "$temp_kafka_time" ] ; then
        servers=$(select_random_servers "$brokers_to_kill" "$actual_max_num_of_server_to_die")
        kafka_restart "--broker=$servers --wait-before-start=$wait_before_start"
        temp_kafka_time=0
      fi
    done
  fi
}

function zk_failure_simulator() {
  temp_zk_time=0
  zk_failure_time=$(get_opt --zk-failure 180 $@)
  wait_before_start=$(get_opt --wait-before-start 10 $@)
  zk_to_kill=$(get_opt --zk-server '' $@)
  min_zk=$(get_opt --min-zk 1 $@)
  ZOOKEEPER_SERVERS_ARRAY=($ZOOKEEPER_SERVERS)
  actual_max_num_of_server_to_die=`expr ${#ZOOKEEPER_SERVERS_ARRAY[@]} - $min_zk`

  if [ "$actual_max_num_of_server_to_die" -gt 0 ] ; then
    h1 "Zookeeper fail in $zk_failure_time"

    while true;
    do
      sleep 1
      (( temp_zk_time++ ))
      if [ "$zk_failure_time" -eq "$temp_zk_time" ] ; then
        servers=$(select_random_servers "$zk_to_kill" "$actual_max_num_of_server_to_die")
        zookeeper_restart "--zk-server=$servers --wait-before-start=$wait_before_start"
        temp_zk_time=0
      fi
    done
  fi

}

function start_simulator() {
  zk_server=$(get_opt --zk-server '' $@)
  kafka_broker=$(get_opt --kafka-broker '' $@) 

  if [ ! -z "$kafka_broker" ]; then
    kafka_failure_simulator $@ &
  fi
  if [ ! -z "$zk_server" ]; then
    zk_failure_simulator $@ &
  fi
  wait
}

parse_hosts_file
# get sub command
COMMAND=$1
shift

if [ "$COMMAND" = "kafka" ] ; then
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "restart" ] ; then
    kafka_restart $@
  else
    print_usage
  fi
elif [ "$COMMAND" = "zookeeper" ] ; then
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "restart" ] ; then
    zookeeper_restart $@
  else
    print_usage
  fi
elif [ "$COMMAND" = "simulate" ] ; then
  start_simulator $@
elif [ "$COMMAND" = "help" ] ; then
  print_usage
else
  print_usage
fi


