CWD=`pwd`
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

cd $DIR
cd ../../

#Set up Scribengin cluster
./installDependencies.sh
cd docker/scribengin/
./docker.sh container clean || true
./docker.sh image clean || true
./scribengin.sh build
./docker.sh image build
./docker.sh container run --zk-server=2 --kafka-server=4
if [[ $OSTYPE == *"darwin"* ]] ; then
  ./docker.sh ip-route
  ./docker.sh update-hosts
else
  sudo ./docker.sh update-hosts
fi



cd $CWD