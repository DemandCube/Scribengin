#!/bin/bash

cygwin=false
ismac=false
case "`uname`" in
  CYGWIN*) cygwin=true;;
  Darwin) ismac=true;;
esac

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

export HADOOP_USER_NAME="neverwinterdp"

APP_DIR=`cd $bin/..; pwd; cd $bin`
JAVACMD=$JAVA_HOME/bin/java

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

JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1536m -XX:-UseSplitVerifier" 
APP_OPT="-Dapp.dir=$APP_DIR -Duser.dir=$APP_DIR"
#LOG_OPT="-Dlog4j.configuration=file:$APP_DIR/config/log4j.properties"

function scribengin_start() {
  MAIN_CLASS="com.neverwinterdp.scribengin.ScribenginLauncher"
  $JAVACMD -Djava.ext.dirs=$APP_DIR/libs $JAVA_OPTS $APP_OPT $LOG_OPT $MAIN_CLASS "$@"
}

function scribengin_shutdown() {
  echo "TODO:............................."
}

function dataflow_submit() {
  echo "TODO:............................."
}

function dataflow_info() {
  echo "TODO:............................."
}

# get command
COMMAND=$1
shift

if [ "$COMMAND" = "scribengin" ] ; then
  # get subcommand
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "start" ] ; then
    scribengin_start $@
  elif [ "$SUB_COMMAND" = "shutdown" ] ; then
    echo "TODO:............................."
  else
    echo "TODO:............................."
  fi
elif [ "$COMMAND" = "dataflow" ] ; then
  # get subcommand
  SUB_COMMAND=$1
  shift
  if [ "$SUB_COMMAND" = "submit" ] ; then
    echo "TODO:............................."
  elif [ "$SUB_COMMAND" = "info" ] ; then
    echo "TODO:............................."
  else
    echo "TODO:............................."
  fi
else
  MAIN_CLASS="com.neverwinterdp.scribengin.Main"
  $JAVACMD -Djava.ext.dirs=$APP_DIR/libs $JAVA_OPTS $APP_OPT $LOG_OPT $MAIN_CLASS "$@"
fi
