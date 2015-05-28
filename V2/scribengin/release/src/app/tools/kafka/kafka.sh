#!/bin/bash

cygwin=false
ismac=false
case "`uname`" in
  CYGWIN*) cygwin=true;;
  Darwin) ismac=true;;
esac

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
echo "bin 1 = $bin"

APP_DIR=`cd $bin/../..; pwd; cd $bin`
JAVACMD=$JAVA_HOME/bin/java
echo "APP_DIR 2 = $APP_DIR"

if [ "x$JAVA_HOME" == "x" ] ; then 
  echo "WARNING JAVA_HOME is not set"
fi

(which $JAVACMD)
isjava=$?

if $ismac && [ $isjava -ne 0 ] ; then
  which java
  if [ $? -eq 0 ] ; then
    JAVACMD=`which java`
    echo "Defaulting to java: $JAVACMD"
  else 
    echo "JAVA Command (java) Not Found Exiting"
    exit 1
  fi
fi

if $cygwin; then
  APP_DIR=`cygpath --absolute --windows "$APP_DIR"`
fi

function kafka_stability_check() {
  MAIN_CLASS="com.neverwinterdp.kafka.tool.KafkaTopicCheckTool"
  $JAVACMD -Djava.ext.dirs=$APP_DIR/libs $JAVA_OPTS $APP_OPT $LOG_OPT $MAIN_CLASS "$@"
}

JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1536m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"

# get command
COMMAND=$1
shift

if [ "$COMMAND" = "info" ] ; then
  echo "TODO: "
elif [ "$COMMAND" = "test" ] ; then
  # get subcommand
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "stability" ] ; then
    kafka_stability_check $@
  elif [ "$SUB_COMMAND" = "performance" ] ; then
    echo "TODO: implement the performance test"
  else
    echo "TODO: print the usage instructions"
  fi
else
  echo "TODO: print the usage instructions"
fi
