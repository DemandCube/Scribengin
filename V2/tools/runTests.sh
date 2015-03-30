#! /bin/sh

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
 
nosetests $SCRIPTPATH/cluster/test/ --with-xunit --xunit-file=clusterTests.xml
