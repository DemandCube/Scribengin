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
  
  #Setting paramiko's logger to be quiet, otherwise too much noise
  logging.getLogger("paramiko").setLevel(logging.WARNING)
  if _debug:
      #Set logging file, overwrite file, set logging level to DEBUG
      logging.basicConfig(filename=_logfile, filemode="w", level=logging.DEBUG)
      logging.getLogger().addHandler(logging.StreamHandler(stdout))
      click.echo('Debug mode is %s' % ('on' if debug else 'off'))
  else:
    #Set logging file, overwrite file, set logging level to INFO
    logging.basicConfig(filename=_logfile, filemode="w", level=logging.INFO)

@mastercommand.command(help="Get Cluster status")
@click.option('--role',  default="",  help="Which role to check on (i.e. kafka, zookeeper, hadoop-master, hadoop-worker)")
def status(role):
  cluster = Cluster()
  if len(role) > 0 :
    cluster = cluster.getServersByRole(role)
  
  click.echo(cluster.getReport())
  
@mastercommand.command(help="Standalone Kafka options")
@click.option('--restart',           is_flag=True, help="restart kafka brokers")
@click.option('--start',             is_flag=True, help="start kafka brokers")
@click.option('--stop',              is_flag=True, help="stop kafka brokers")
@click.option('--force-stop',        is_flag=True, help="kill -9 kafka on brokers")
@click.option('--clean',             is_flag=True, help="Clean old kafka data")
@click.option('--brokers',           default="",   help="Which kafka brokers to effect (command separated list)")
@click.option('--wait-before-start', default=0,    help="Time to wait before restarting kafka server (seconds)")
@click.option('--wait-before-kill',  default=0,    help="Time to wait before force killing Kafka process (seconds)")
def kafka(restart, start, stop, force_stop, clean, brokers, wait_before_start, wait_before_kill):
  cluster = Cluster()
  
  if len(brokers) > 0 :
    brokerList = brokers.split(",")
    cluster = cluster.getServersByHostname(brokerList)
  
  if(restart or stop):
    logging.debug("Shutting down Kafka")
    cluster.shutdownKafka()
  
  if(force_stop):
    logging.debug("Waiting for "+str(wait_before_kill)+" seconds")
    sleep(wait_before_kill)
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
  click.echo(cluster.getReport())  
  

@mastercommand.command(help="Standalone ZK commands")
@click.option('--restart',           is_flag=True, help="restart ZK nodes")
@click.option('--start',             is_flag=True, help="start ZK nodes")
@click.option('--stop',              is_flag=True, help="stop ZK nodes")
@click.option('--force-stop',        is_flag=True, help="kill -9 ZK on brokers")
@click.option('--clean',             is_flag=True, help="Clean old ZK data")
@click.option('--zk-servers',        is_flag=True, help="Which ZK nodes to effect (command separated list)")
@click.option('--wait-before-start', default=0,     help="Time to wait before starting ZK server (seconds)")
@click.option('--wait-before-kill',  default=0,     help="Time to wait before force killing ZK process (seconds)")
def zookeeper(restart, start, stop, force_stop, clean, zk_servers, wait_before_start, wait_before_kill):
  cluster = Cluster()
  if(restart or stop):
    logging.debug("Shutting down Zookeeper")
    cluster.shutdownZookeeper()
  
  if(force_stop):
    logging.debug("Waiting for "+str(wait_before_kill)+" seconds")
    sleep(wait_before_kill)
    logging.debug("Force Killing Zookeeper")
    cluster.killZookeeper()
  
  if(clean):
    logging.debug("Cleaning Zookeeper")
    cluster.cleanZookeeper()
  
  if(restart or start):
    logging.debug("Waiting for "+str(wait_before_start)+" seconds")
    sleep(wait_before_start)
    logging.debug("Starting Zookeeper")
    cluster.startZookeeper()
  
  click.echo(cluster.getReport())

@mastercommand.command(help="Failure Simulation")
@click.option('--zk-failure', default=180, help="Time interval (in seconds) to fail Zookeeper nodes")
@click.option('--kafka-failure', default=180, help="Time interval (in seconds) to fail Kafka nodes")
@click.option('--wait-before-start', default=180, help="Time to wait (in seconds) before starting Kafka or Zookeeper")
@click.option('--zk-server', default="", help="Zookeeper servers to effect.  Command separated list (i.e. --zk-server zk1,zk2,zk3)")
@click.option('--kafka-broker', default="", help="Kafka brokers to effect.  Command separated list (i.e. --kafka-broker kafka1,kafka2)")
@click.option('--min-zk', default=1, help="Minimum number of Zookeeper nodes that must stay up")
@click.option('--min-kafka', default=1, help="Minimum number of Kafka nodes that must stay up")
def failure(zk_failure, kafka_failure, wait_before_start, zk_server, kafka_broker, min_zk, min_kafka):
  cluster = Cluster()
  
  click.echo(cluster.getReport())

if __name__ == '__main__':
  mastercommand()
