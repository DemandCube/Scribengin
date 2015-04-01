import click, logging, multiprocessing, signal
from sys import path, stdout, exit
from os.path import join, dirname, abspath
from time import sleep
path.insert(0, join(dirname(dirname(abspath(__file__))), "cluster"))
from Cluster import Cluster  #@UnresolvedImport
import kafkaFailureSimulator, zkFailureSimulator


_debug = False
_logfile = ''
_jobs = []

@click.group(chain=True)
@click.option('--debug/--no-debug', default=False, help="Turn debugging on")
@click.option('--logfile', default='clusterCommander.log', help="Log file to write to")
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
  
@mastercommand.command("kafka", help="Kafka commands")
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
  

@mastercommand.command("zookeeper",help="Zookeeper commands")
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

@mastercommand.command("kafkafailure",help="Failure Simulation for Kafka")
@click.option('--failure-interval',              default=180,  help="Time interval (in seconds) to fail server")
@click.option('--wait-before-start',             default=180,  help="Time to wait (in seconds) before starting server")
@click.option('--servers',                       default="",   help="Servers to effect.  Command separated list (i.e. --servers zk1,zk2,zk3)")
@click.option('--min-servers',                   default=1,    help="Minimum number of servers that must stay up")
@click.option('--servers-to-fail-simultaneously', default=1,   help="Number of servers to kill simultaneously")
@click.option('--kill-method',                   default='kill', type=click.Choice(['restart', 'kill', "random"]), help="Server kill method.  Restart is clean, kill uses kill -9, random switches randomly")
@click.option('--initial-clean',                 is_flag=True, help="If enabled, will run a clean operation before starting the failure simulation")
def kafkafailure(failure_interval, wait_before_start, servers, min_servers, servers_to_fail_simultaneously, kill_method, initial_clean):
  global _jobs
  if len(servers) < 1:
    raise ValueError("--servers is not specified!")
    return
  
  p = multiprocessing.Process(name="KafkaFailure",
                              target=kafkaFailureSimulator.kafkaFailure, 
                              args=(failure_interval, wait_before_start, servers, min_servers, servers_to_fail_simultaneously, kill_method, initial_clean))
  _jobs.append(p)
  p.start()
  
  for job in _jobs:
    job.join()

@mastercommand.command("zookeeperfailure",help="Failure Simulation for Zookeeper")
@click.option('--failure-interval',              default=180,  help="Time interval (in seconds) to fail server")
@click.option('--wait-before-start',             default=180,  help="Time to wait (in seconds) before starting server")
@click.option('--servers',                       default="",   help="Servers to effect.  Command separated list (i.e. --servers zk1,zk2,zk3)")
@click.option('--min-servers',                   default=1,    help="Minimum number of servers that must stay up")
@click.option('--server-to-fail-simultaneously', default=1,    help="Number of servers to kill simultaneously")
@click.option('--kill-method',                   default='kill', type=click.Choice(['restart', 'kill', "random"]), help="Server kill method.  Restart is clean, kill uses kill -9, random switches randomly")
@click.option('--initial-clean',                 is_flag=True, help="If enabled, will run a clean operation before starting the failure simulation")
def zookeeperfailure(failure_interval, wait_before_start, servers, min_servers, servers_to_fail_simultaneously, kill_method, initial_clean):
  global _jobs
  if len(servers) < 1:
    raise ValueError("--servers is not specified!")
    return
  
  p = multiprocessing.Process(name="ZookeeperFailure",
                              target=zkFailureSimulator.zkFailure, 
                              args=(failure_interval, wait_before_start, servers, min_servers, servers_to_fail_simultaneously, kill_method, initial_clean))
  _jobs.append(p)
  p.start()
  
  for job in _jobs:
    job.join()

@mastercommand.command("monitor",help="Monitor Cluster status")
@click.option('--update-interval', default=30, help="Time interval (in seconds) to wait between updating cluster status")
def monitor(update_interval):
  """
  Prints the cluster report
  """
  cluster = Cluster()
  while True:
    click.echo(cluster.getReport())
    click.echo("\n\n")
    sleep(update_interval)

def catchSignal(signal, frame):
  """
  Make sure we clean up when ctrl+c is hit
  """
  global _jobs
  for job in _jobs:
    try:
      job.terminate()
    except:
      pass
    try:
      job.join()
    except:
      pass
  exit(0)    


if __name__ == '__main__':
  #Set the signal handler
  signal.signal(signal.SIGINT, catchSignal)
  #Parse commands and run
  mastercommand()
