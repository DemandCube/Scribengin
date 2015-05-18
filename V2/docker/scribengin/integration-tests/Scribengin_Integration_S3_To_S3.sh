#Set up docker images
DOCKERSCRIBEDIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
$DOCKERSCRIBEDIR/../docker.sh cluster --clean-containers --run-containers --deploy-scribengin --start-cluster
scp -o "StrictHostKeyChecking no" -r /root/.aws neverwinterdp@hadoop-master:/home/neverwinterdp/

#make folder for test results
mkdir testresults

#Give everything time to come up
sleep 5

  echo "testing existence of .aws folder on local host"
  if [ -d /root/.aws ] ; then 
    echo "pasword exists on host !!!!"
  else
    echo "we have the wrong host location."
  fi


 echo "testing existence of .aws folder on remote host"
is_exists=`ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "if [ -e /home/neverwinterdp/.aws/credentials ] ; then echo '0';else echo '1'; fi"`
if [ $is_exists == '0' ]; then
  echo "Credentials file exists"
else
  echo "credentials file does not exists"
fi

  echo "testing existence of sunjce file on remote host"
  if [ -e $JAVA_HOME/jre/lib/ext ] ; then 
    echo "sunjce exists !!!!"
  else
    echo "sunjce exists not."
  fi


ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh scribengin info"
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && ./bin/shell.sh vm info"


#Run dataflow
UUID=$(cat /proc/sys/kernel/random/uuid)
ssh -o "StrictHostKeyChecking no" neverwinterdp@hadoop-master "cd /opt/scribengin/scribengin && \
          ./bin/shell.sh dataflow-test s3-to-s3 \
                 --dataflow-name  s3-to-s3 \
                 --dataflow-id    s3-to-s3-1 \
                 --worker 3 \
                 --executor-per-worker 1 \
                 --duration 90000 \
                 --task-max-execute-time 10000 \
                 --source-location jenkins-dataflow-test-$UUID \
                 --source-name dataflow-test \
                 --source-num-of-stream 1    \
                 --source-max-records-per-stream 100 \
                 --sink-location jenkins-dataflow-test-$UUID  \
                 --sink-name dataflow-test  \
                 --print-dataflow-info -1 \
                 --debug-dataflow-task  \
                 --debug-dataflow-vm  \
                 --debug-dataflow-activity  \
                 --junit-report S3_IntegrationTest.xml \
                 --dump-registry"

#Get results
scp -o stricthostkeychecking=no neverwinterdp@hadoop-master:/opt/scribengin/scribengin/S3_IntegrationTest.xml ./testresults/

#Clean up
$DOCKERSCRIBEDIR/.././docker.sh cluster --clean-containers
