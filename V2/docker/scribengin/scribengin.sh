#!/bin/bash

function h1() {
  echo ""
  echo "###########################################################################################################"
  echo "$@"
  echo "###########################################################################################################"
}

function printUsage() {
  usage=`basename $0`
  usage="$usage USAGE"
  h1 $usage
  echo "./scribengin.sh build [--with-test]     Build Scribengin V2.  Optionally run tests."
  echo "./scribengin.sh deploy [--clean]        Deploy Scribengin V2 to Docker cluster.  Optionally run cluster.sh clean on all nodes."
}

function build(){
  OPTION=$1
  
  #Omit the the tests by default
  if [ "$OPTION" = "--with-test" ] ; then
    command="../gradlew clean build install release"
  else
    command="../gradlew clean build install release -x test"
  fi
  
  #Build Scribengin
  pwd=`pwd`
  h1 "Building Scribengin/V2"
  cd ../../
  $command
  cd "$pwd"
  
  #Build and release the javaagent
  #cd ../../jvmagent/registry/
  #../../../gradlew clean build release -x test
  #cd "$pwd"
  
  #Release the project
  cd ../../release
  ../../gradlew clean release
  cd "$pwd"
  if [ -d ./bootstrap/post-install/release ] ; then
    rm -rf ./bootstrap/post-install/release
  fi
  #Move release/build/release here
  cp -R -f ../../release/build/release ./bootstrap/post-install/release
}


function deploy(){
  OPTION=$1
  #Stop all running java processes across the cluster
  ssh neverwinterdp@hadoop-master -C "/opt/cluster.sh exec \"pkill -9 java\""
  
  if [ "$OPTION" = "--clean" ] ; then
    ssh neverwinterdp@hadoop-master -C "/opt/cluster.sh clean"
  fi
  
  #Copy 
  scp -r ../../release/build/release neverwinterdp@hadoop-master:/opt/scribengin
  ssh neverwinterdp@hadoop-master -C "yes | /opt/cluster.sh sync"
}

# get command
COMMAND=$1
shift

if [ "$COMMAND" = "build" ] ; then
  build $@
elif [ "$COMMAND" = "deploy" ] ; then
  deploy $@
else
  printUsage
fi