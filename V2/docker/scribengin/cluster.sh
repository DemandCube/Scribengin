#!/bin/bash

BIN_DIR=`dirname "$0"`
BIN_DIR=`cd "$BIN_DIR"; pwd`

function printHeader() {
  echo ""
  echo "###########################################################################################################"
  echo "$@"
  echo "###########################################################################################################"
}

function build_image() {
  printHeader "Build the os image with the preinstalled requirements"
  echo "Prepare the temporary configuration files"
  mkdir ./tmp
  cat ~/.ssh/id_rsa.pub > ./tmp/authorized_keys
  ssh-keygen -t rsa  -P "" -f tmp/id_rsa

  docker build -t ubuntu:scribengin $BIN_DIR
  echo "Clean the temporary configuration files"
  rm -rf ./tmp
}

function clean_image() {
  printHeader "Clean the images"
  docker rmi -f ubuntu:scribengin
}

function runCluster() {
  printHeader "Run the cluster"
  NAME="hadoop"
  #docker run -d -p 22 --privileged -h hadoop --name $NAME ubuntu:scribengin /bin/sh -c "while true; do echo Hello world; sleep 1; done"
  docker run -d -p 22 --privileged -h hadoop --name $NAME ubuntu:scribengin
  docker port $NAME
}

function runHadoop() {
  printHeader "Run hadoop vm machines"
  HADOOP_VMS="hadoop1 hadoop2 hadoop3 hadoop4"
  for NAME in $HADOOP_VMS
  do
    VM_NAME="${NAME}.scribengin"
    docker run -d -p 22 -p 50070 --privileged -h "$VM_NAME" --name "$VM_NAME" ubuntu:scribengin
  done
  docker ps
}

function container_login() {
  printHeader "Login to the instance $@"
  docker exec -i -t $1 bash 
}

function login_ssh_port() {
  docker port $1 22 | sed 's/.*://'
}

function do_ssh() {
  printHeader "Login to the instance $@"
  arr=(${1//@/ })

  port=$(login_ssh_port ${arr[1]})
  ssh ${arr[0]}@$(boot2docker ip) -p $port
}

function do_scp() {
  printHeader "scp $@"

  HOST=$(echo "$@" | sed "s/.*@\(.*\):.*$/\1/")
  PORT=$(login_ssh_port $HOST)
  BOOT2DOCKER_IP=$(boot2docker ip)
  NEW_ARGS=$(echo "$@" | sed "s/@\(.*\):/@$BOOT2DOCKER_IP:/")
  echo "NEW ARGS = $NEW_ARGS" 
  #scp -P $PORT ${arr[0]}@$(boot2docker ip)
  echo "scp -P $PORT $NEW_ARGS"
  scp -P $PORT $NEW_ARGS
}

function container_ids() {
  docker_ps_result=$(docker ps)
  entries=()
  while read -r line; do
    entries+=("$line")
  done <<< "$docker_ps_result"

  entries_len=${#entries[@]}
  IDS=""
  for(( i=1; i<${entries_len}; i++ ));
  do
    #extract the container id
    if (( i > 1 )); then
      IDS+=' '
    fi
    IDS+=$(echo ${entries[$i]} | cut -d' ' -f 1)
  done
  echo "$IDS"
}

function container_update_hosts() {
  printHeader "Update /etc/hosts file"
  HOSTS=$'## scribengin server ##\n'
  HOSTS+=$'127.0.0.1 localhost\n'
  for container_id in $(container_ids); do
    #extract the container name
    container_name=$(docker inspect -f {{.Config.Hostname}} $container_id)
    container_domain=$(docker inspect -f {{.Config.Domainname}} $container_id)
    #extract the container ip
    container_ip=$(docker inspect -f "{{ .NetworkSettings.IPAddress }}" $container_id)
    #extract the container running state
    container_running=$(docker inspect -f {{.State.Running}} $container_id)
    HOSTS+="$container_ip $container_name $container_name.$container_domain"
    HOSTS+=$'\n'
    echo "container id = $container_id, container name = $container_name, container ip = $container_ip, container running = $container_running"
  done

  for container_id in $(container_ids); do
    #extract the container name
    container_name=$(docker inspect -f {{.Config.Hostname}} $container_id)
    echo "Update /etc/hosts for $container_name"
    ssh -o StrictHostKeyChecking=no -p $(login_ssh_port $container_id) root@$(boot2docker ip) "echo '$HOSTS'  > /etc/hosts"
  done
}

function container_clean() {
  for container_id in $(container_ids); do
    container_name=$(docker inspect -f {{.Config.Hostname}} $container_id)
    docker rm -f $container_id
    echo "Remove the instance $container_name"
  done
}

function printUsage() {
  echo "Cluster command options: "
  echo "  Command image: "
  echo "    build                 : To build the ubuntu os image with the preinstalled requirement"
  echo "    clean                 : To remove the image"
  echo "  Command container:"
  echo "    run                   : To run the containers"
  echo "    clean                 : To remove and destroy all the running containers"
  echo "    login                 : To login the given container with the root user"
  echo "    update-hosts          : To update the /etc/hosts in all the running containers"
}

# get command
COMMAND=$1
shift

if [ "$COMMAND" = "image" ] ; then
  # get subcommand
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "build" ] ; then
    build_image $@
  elif [ "$SUB_COMMAND" = "clean" ] ; then
    clean_image $@
  else
    printUsage
  fi
elif [ "$COMMAND" = "container" ] ; then
  # get subcommand
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "clean" ] ; then
    container_clean $@
  elif [ "$SUB_COMMAND" = "run" ] ; then
    runHadoop
    container_update_hosts $@
  elif [ "$SUB_COMMAND" = "update-hosts" ] ; then
    container_update_hosts $@
  elif [ "$SUB_COMMAND" = "login" ] ; then
    container_login $@
  else
    printUsage
  fi
elif [ "$COMMAND" = "ssh" ] ; then
  do_ssh $@
elif [ "$COMMAND" = "scp" ] ; then
  do_scp $@
else
  echo "cluster command options: "
  echo "  build                 : To build the ubuntu os image with the preinstalled requirement"
  echo "  run                   : To run the cluster"
fi

