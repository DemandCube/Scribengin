#!/bin/bash

cygwin=false
ismac=false
case "`uname`" in
  CYGWIN*) cygwin=true;;
  Darwin) ismac=true;;
esac

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

ES_HOME=`cd $bin/..; pwd; cd $bin`
JAVACMD=$JAVA_HOME/bin/java

JAVA_OPTS="-Xshare:auto -Xms128m -Xmx1536m -XX:-UseSplitVerifier" 
LOG_OPT="-Dlog4j.configuration=file:$ES_HOME/config/log4j.properties"
ES_OPT="-Dapp.dir=$ES_HOME -Duser.dir=$ES_HOME"


# get command
COMMAND=$1
shift

if [ "$COMMAND" = "run" ] ; then
  MAIN_CLASS=com.neverwinterdp.es.Main
  $JAVACMD -Djava.ext.dirs=$ES_HOME/libs:/usr/java/jdk1.7.0_45/jre/lib/ext $JAVA_OPTS $ES_OPT $MAIN_CLASS \
     --es:cluster.name=neverwinterdp \
     --es:server.name=$HOSTNAME \
     --es:path.data=$ES_HOME/data 
else
  MAIN_CLASS=com.neverwinterdp.es.Main
  nohup $JAVACMD -Djava.ext.dirs=$ES_HOME/libs:/usr/java/jdk1.7.0_45/jre/lib/ext $JAVA_OPTS $ES_OPT $LOG_OPT $MAIN_CLASS \
     --es:cluster.name=neverwinterdp \
     --es:server.name=$HOSTNAME \
     --es:path.data=$ES_HOME/data  "$@" <&- &>/dev/null &
fi

