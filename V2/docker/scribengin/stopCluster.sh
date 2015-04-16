CWD=`pwd`
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR

#Clean up
./docker.sh container clean || true
./docker.sh image clean || true

cd $CWD