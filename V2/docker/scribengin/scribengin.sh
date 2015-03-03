#!/bin/bash
set -x

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

function buildScribenginV2(){
  OPTION=$1
  
  if [ "$OPTION" = "--with-test" ] ; then
    command="../gradlew clean build install"
  else
    command="../gradlew clean build install -x test"
  fi
  
  pwd=`pwd`
  h1 "Building Scribengin/V2"
  cd ../../
  $command
  cd "$pwd"
}

function deployScribenginV2(){
  OPTION=$1
  ssh neverwinterdp@hadoop-master -C "/opt/cluster.sh exec \"pkill -9 java\""
  if [ "$OPTION" = "--clean" ] ; then
    ssh neverwinterdp@hadoop-master -C "/opt/cluster.sh clean"
  fi
  scp -r V2/release/build/release neverwinterdp@hadoop-master:/opt/scribengin
  ssh neverwinterdp@hadoop-master -C "yes | /opt/cluster.sh sync"
}

# get command
COMMAND=$1
shift

if [ "$COMMAND" = "build" ] ; then
  buildScribenginV2 $@
elif [ "$COMMAND" = "deploy" ] ; then
  deployScribenginV2 $@
else
  printUsage
fi