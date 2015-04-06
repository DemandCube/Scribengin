#! /bin/sh

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`

XUNITFILE=$1
TESTCOMMAND="nosetests -s $SCRIPTPATH/test/"
if [ $XUNITFILE ] && [ $XUNITFILE != "" ]
then
  TESTCOMMAND="$TESTCOMMAND --with-xunit --xunit-file=$XUNITFILE"
fi

echo "Running: $TESTCOMMAND"
$TESTCOMMAND