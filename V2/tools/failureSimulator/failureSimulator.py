import click, logging
from sys import path,stdout
from os.path import join,dirname,abspath
from time import sleep
path.insert(0, join(dirname(dirname(abspath(__file__))), "cluster"))
from Cluster import Cluster  #@UnresolvedImport

_debug = False
_logfile = ''

@click.group()
@click.option('--debug/--no-debug', default=False, help="Turn debugging on")
@click.option('--logfile', default='failureSimulator.log', help="Log file to write to")
def mastercommand(debug, logfile):
  global _debug, _logfile
  _debug = debug
  _logfile = logfile
  if _debug:
      #Set logging file, overwrite file, set logging level to DEBUG
      logging.basicConfig(filename=_logfile, filemode="w", level=logging.DEBUG)
      logging.getLogger().addHandler(logging.StreamHandler(stdout))
      click.echo('Debug mode is %s' % ('on' if debug else 'off'))
  else:
    #Set logging file, overwrite file, set logging level to INFO
    logging.basicConfig(filename=_logfile, filemode="w", level=logging.INFO)

@mastercommand.command(help="Standalone Kafka options")
@click.option('--restart',           is_flag=True, help="restart kafka brokers")
@click.option('--start',             is_flag=True, help="start kafka brokers")
@click.option('--stop',              is_flag=True, help="stop kafka brokers")
@click.option('--force-stop',        is_flag=True, help="kill -9 kafka on brokers")
@click.option('--clean',             is_flag=True, help="Clean old kafka data")
@click.option('--brokers',           is_flag=True, help="Which kafka brokers to effect (command separated list)")
@click.option('--wait-before-start', default=0,     help="Time to wait before restarting kafka server (seconds)")
def kafka(restart, start, stop, force_stop, clean, brokers, wait_before_start):
  cluster = Cluster()
  if(restart or stop):
    logging.debug("Shutting down Kafka")
    cluster.shutdownKafka()
  
  if(force_stop):
    logging.debug("Force Killing Kafka")
    cluster.killKafka()
  
  if(clean):
    logging.debug("Cleaning Kafka")
    cluster.cleanKafka()
  
  if(restart or start):
    logging.debug("Waiting for "+str(wait_before_start)+" seconds")
    sleep(wait_before_start)
    logging.debug("Starting Kafka")
    cluster.startKafka()
    
  

@mastercommand.command(help="Standalone ZK commands")
@click.option('--restart',           default=False, help="restart ZK nodes")
@click.option('--start',             default=False, help="start ZK nodes")
@click.option('--stop',              default=False, help="stop ZK nodes")
@click.option('--clean',             default=False, help="Clean old ZK data")
@click.option('--zk-servers',        default=False, help="Which ZK nodes to effect (command separated list)")
@click.option('--wait-before-start', default=10,    help="Time to wait before starting ZK server (seconds)")
def zookeeper():
  print "ZK!"

@mastercommand.command(help="Failure Simulation")
def failure():
  print "failure!"

if __name__ == '__main__':
  mastercommand()

#   echo "kafka                           :Main command to invoke kafka related operations"
#   echo "  restart                       :To restart any kafka server"
#   echo "    --broker                    :Kafka brokers, Multiple values can be given in comma seperated value ( Example: --broker=kafka-1,kafka-2 )"
#   echo "    --wait-before-start         :Time to wait before start kafka server, Measured in seconds ( Example: --wait-before-start=10 )"
#   echo "    --clean      :Clean kafka old datas"
#   echo "zookeeper                       :Main command to invoke zookeeper related operations"
#   echo "  restart                       :To restart any zookeeper server"
#   echo "    --zk-server                 :Zokeeper servers, Multiple values can be given in comma seperated value ( Example: --zk-server=zookeeper-1,zookeeper-2 )"
#   echo "    --wait-before-start         :Time to wait before start zookeeper server, Measured in seconds ( Example: --wait-before-start=10 )"
#   echo "    --clean                     :Clean zookeeper old datas"
#   echo "simulate      :To simulate failures for kafka and zookeeper servers"
#   echo "  --zk-failure                  :Time interval to fail zookeeper server ( Example: zk-failure=180 )"
#   echo "  --kafka-failure               :Time interval to fail kafka broker ( Example: kafka-failure=60 )"
#   echo "  --wait-before-start           :Time to wait before start kafka or zookeeper, Measured in seconds ( Example: wait-before-start=10 )"  
#   echo "  --zk-server                   :Zokeeper servers, Multiple values can be given in comma seperated value ( Example: zk-server=zookeeper-1,zookeeper-2 )"
#   echo "  --kafka-broker                :Kafka brokers, Multiple values can be given in comma seperated value ( Example: kafka-broker=kafka-1,kafka-2 )"
#   echo "  --min-zk      :The minimum number of ZK nodes that must always stay up. (This is optional, by default it will keep 1 Zk node always alive)"
#   echo "  --min-kafka                   :The minimum number of kafka brokers that must always stay up. (This is optional, by default it will keep 1 kafka broker always alive)"
#   echo " "
