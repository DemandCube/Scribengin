#! /bin/sh
""":"
exec python $0 ${1+"$@"}
"""

import click, logging, multiprocessing, signal, os
from sys import stdout, exit
from time import sleep

from failure.FailureSimulator import ZookeeperFailure,KafkaFailure,DataFlowFailure
from Cluster import Cluster

_debug = False
_logfile = ''
_jobs = []

@click.group(chain=True)
@click.option('--debug/--no-debug', default=False, help="Turn debugging on")
@click.option('--logfile', default='failure.log', help="Log file to write to")
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

@mastercommand.command("vmmaster", help="VmMaster commands")
@click.option('--restart',           is_flag=True, help="restart VmMaster")
@click.option('--start',             is_flag=True, help="start VmMaster")
@click.option('--stop',              is_flag=True, help="stop VmMaster")
@click.option('--force-stop',        is_flag=True, help="kill VmMaster")
@click.option('--wait-before-start', default=0,    help="Time to wait before restarting VmMaster (seconds)")
@click.option('--wait-before-report', default=5,    help="Time to wait before restarting reporting cluster status (seconds)")
def vmmaster(restart, start, stop,force_stop, wait_before_start, wait_before_report):
  cluster = Cluster()
  
  if(restart or stop):
    logging.debug("Shutting down VmMaster")
    cluster.shutdownVmMaster()
  
  if(force_stop):
    logging.debug("Killing VmMaster")
    cluster.killVmMaster()
  
  if(restart or start):
    logging.debug("Waiting for "+str(wait_before_start)+" seconds")
    sleep(wait_before_start)
    logging.debug("Starting VmMaster")
    cluster.startVmMaster()
  
  logging.debug("Waiting for "+str(wait_before_report)+" seconds")
  sleep(wait_before_report)
  #click.echo(cluster.getReport())  

@mastercommand.command("scribengin", help="Scribengin commands")
@click.option('--restart',           is_flag=True, help="restart Scribengin")
@click.option('--start',             is_flag=True, help="start Scribengin")
@click.option('--stop',              is_flag=True, help="stop Scribengin")
@click.option('--force-stop',        is_flag=True, help="kill Scribengin")
@click.option('--wait-before-start', default=0,    help="Time to wait before restarting Scribengin (seconds)")
@click.option('--wait-before-report', default=5,    help="Time to wait before restarting reporting cluster status (seconds)")
@click.option('--build',   is_flag=True, help="Build Scribengin")
@click.option('--with-test',   is_flag=True, help="Build Scribengin with test")
@click.option('--deploy',   is_flag=True, help="Deploy Scribengin")
@click.option('--aws-credential-path', default="", help="Deploy Scribengin with aws credential. (--aws-credential-path='/root/.aws')")
@click.option('--clean',   is_flag=True, help="Clean cluster")
def scribengin(restart, start, stop, force_stop, wait_before_start, wait_before_report, build, with_test, deploy, aws_credential_path, clean):
  cluster = Cluster()
  
  if(build):
    cluster.scribenginBuild(with_test)
    
  if(deploy):
    cluster.scribenginDeploy("hadoop-master", aws_credential_path, clean)
      
  if(restart or stop):
    logging.debug("Shutting down Scribengin")
    cluster.shutdownScribengin()
  
  if(force_stop):
    logging.debug("Killing Scribengin")
    cluster.killScribengin()
    
  if(restart or start):
    logging.debug("Waiting for "+str(wait_before_start)+" seconds")
    sleep(wait_before_start)
    logging.debug("Starting Scribengin")
    cluster.startScribengin()
  
  logging.debug("Waiting for "+str(wait_before_report)+" seconds")
  sleep(wait_before_report)
  #click.echo(cluster.getReport())  

@mastercommand.command("cluster", help="Cluster commands")
@click.option('--restart',             is_flag=True, help="restart cluster")
@click.option('--start',               is_flag=True, help="start cluster")
@click.option('--stop',                is_flag=True, help="stop cluster")
@click.option('--force-stop',          is_flag=True, help="kill cluster")
@click.option('--clean',               is_flag=True, help="Clean old cluster data")
@click.option('--sync',                default="",   help="Sync cluster datas from the given hostname")
@click.option('--wait-before-start',   default=0,    help="Time to wait before restarting cluster (seconds)")
@click.option('--wait-before-kill',    default=0,    help="Time to wait before force killing cluster (seconds)")
@click.option('--kafka-server-config', default='/opt/kafka/config/default.properties', help='Kafka server configuration template path, default is /opt/kafka/config/default.properties', type=click.Path(exists=False))
@click.option('--zookeeper-server-config',  default='/opt/zookeeper/conf/zoo_sample.cfg', help='Zookeeper configuration template path, default is /opt/zookeeper/conf/zoo_sample.cfg', type=click.Path(exists=False))
@click.option('--execute',                           help='execute given command on all nodes')
def cluster(restart, start, stop, force_stop, clean, sync, wait_before_start, wait_before_kill, kafka_server_config, zookeeper_server_config, execute):
  cluster = Cluster()
      
  if(sync != ""):
    #Validating hostname
    hostname = sync
    if hostname in cluster.getHostnames():
      cluster.sync(sync)
    else:
      raise click.BadParameter("Host \"" + sync + "\" does not exist in the cluster.", param_hint = "--sync")
    
  if(execute is not None):
    cluster.sshExecute(execute)
    
  if(restart or stop):
    logging.debug("Shutting down Cluster")
    cluster.shutdownCluster()
  
  if(force_stop):
    logging.debug("Waiting for "+str(wait_before_kill)+" seconds")
    sleep(wait_before_kill)
    logging.debug("Force Killing Cluster")
    cluster.killCluster()
  
  if(clean):
    logging.debug("Cleaning Kafka")
    cluster.cleanCluster()

  if(restart or start):
    logging.debug("Waiting for "+str(wait_before_start)+" seconds")
    if not os.path.exists(kafka_server_config):
      raise click.BadParameter("Path \"" + kafka_server_config + "\" does not exist.", param_hint = "--kafka-server-config")
    
    if not os.path.exists(zookeeper_server_config):
      raise click.BadParameter("Path \"" + zookeeper_server_config + "\" does not exist.", param_hint = "--zookeeper-server-config")
  
    sleep(wait_before_start)
    logging.debug("Starting Cluster")
    cluster.paramDict["server_config"] = kafka_server_config
    cluster.paramDict["zoo_cfg"] = zookeeper_server_config
    cluster.startCluster()
    
  #click.echo(cluster.getReport())  
  
@mastercommand.command("kafka", help="Kafka commands")
@click.option('--restart',           is_flag=True, help="restart kafka brokers")
@click.option('--start',             is_flag=True, help="start kafka brokers")
@click.option('--stop',              is_flag=True, help="stop kafka brokers")
@click.option('--force-stop',        is_flag=True, help="kill -9 kafka on brokers")
@click.option('--clean',             is_flag=True, help="Clean old kafka data")
@click.option('--start-with-spare',  is_flag=True, help="Start zookeeper cluster with spare zookeeper nodes")
@click.option('--brokers',           default="",   help="Which kafka brokers to effect (command separated list)")
@click.option('--wait-before-start', default=0,    help="Time to wait before restarting kafka server (seconds)")
@click.option('--wait-before-kill',  default=0,    help="Time to wait before force killing Kafka process (seconds)")
@click.option('--server-config',     default='/opt/kafka/config/default.properties', help='Kafka server configuration template path, default is /opt/kafka/config/default.properties', type=click.Path(exists=True))
def kafka(restart, start, stop, force_stop, clean, start_with_spare, brokers, wait_before_start, wait_before_kill, server_config):
  cluster = Cluster()
  
  if len(brokers) > 0 :
    brokerList = brokers.split(",")
    cluster = cluster.getServersByHostname(brokerList)
  
  if(restart or stop):
    logging.debug("Shutting down Kafka")
    cluster.shutdownKafka()
    cluster.shutdownSpareKafka()
    
  if(force_stop):
    logging.debug("Waiting for "+str(wait_before_kill)+" seconds")
    sleep(wait_before_kill)
    logging.debug("Force Killing Kafka")
    cluster.killKafka()
    cluster.killSpareKafka()
  
  if(clean):
    logging.debug("Cleaning Kafka")
    cluster.cleanKafka()
    cluster.cleanSpareKafka()
  
  if(restart or start):
    logging.debug("Waiting for "+str(wait_before_start)+" seconds")
    sleep(wait_before_start)
    logging.debug("Starting Kafka")
    cluster.paramDict["server_config"] = server_config
    cluster.startKafka()
    if start_with_spare:
      cluster.startSpareKafka()
  #click.echo(cluster.getReport())  
  

@mastercommand.command("zookeeper",help="Zookeeper commands")
@click.option('--restart',           is_flag=True, help="restart ZK nodes")
@click.option('--start',             is_flag=True, help="start ZK nodes")
@click.option('--stop',              is_flag=True, help="stop ZK nodes")
@click.option('--force-stop',        is_flag=True, help="kill -9 ZK on brokers")
@click.option('--clean',             is_flag=True, help="Clean old ZK data")
@click.option('--zk-servers',        is_flag=True, help="Which ZK nodes to effect (command separated list)")
@click.option('--wait-before-start', default=0,     help="Time to wait before starting ZK server (seconds)")
@click.option('--wait-before-kill',  default=0,     help="Time to wait before force killing ZK process (seconds)")
@click.option('--zoo-cfg',           default='/opt/zookeeper/conf/zoo_sample.cfg', help='Zookeeper configuration template path, default is /opt/zookeeper/conf/zoo_sample.cfg', type=click.Path(exists=True))
def zookeeper(restart, start, stop, force_stop, clean, zk_servers, wait_before_start, wait_before_kill, zoo_cfg):
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
    cluster.cleanSpareZookeeper()
  
  if(restart or start):
    logging.debug("Waiting for "+str(wait_before_start)+" seconds")
    sleep(wait_before_start)
    logging.debug("Starting Zookeeper")
    cluster.paramDict["zoo_cfg"] = zoo_cfg
    cluster.startZookeeper()
  #click.echo(cluster.getReport())

@mastercommand.command("hadoop",help="Hadoop commands")
@click.option('--restart',           is_flag=True, help="restart hadoop nodes")
@click.option('--start',             is_flag=True, help="start hadoop nodes")
@click.option('--stop',              is_flag=True, help="stop hadoop nodes")
@click.option('--force-stop',        is_flag=True, help="kill -9 hadoop on brokers")
@click.option('--clean',             is_flag=True, help="Clean old hadoop data")
@click.option('--hadoop-nodes',      is_flag=True, help="Which hadoop nodes to effect (command separated list)")
@click.option('--wait-before-start', default=0,    help="Time to wait before starting ZK server (seconds)")
@click.option('--wait-before-kill',  default=0,    help="Time to wait before force killing ZK process (seconds)")
def hadoop(restart, start, stop, force_stop, clean, hadoop_nodes, wait_before_start, wait_before_kill):
  cluster = Cluster()
  if(restart or stop):
    logging.debug("Shutting down Hadoop")
    cluster.shutdownHadoopWorker()
    cluster.shutdownHadoopMaster()
  
  if(force_stop):
    logging.debug("Waiting for "+str(wait_before_kill)+" seconds")
    sleep(wait_before_kill)
    logging.debug("Force Killing Hadoop")
    cluster.killHadoopWorker()
    cluster.killHadoopMaster()
  
  if(clean):
    logging.debug("Cleaning Hadoop")
    cluster.cleanHadoopWorker()
    cluster.cleanHadoopMaster()
  
  if(restart or start):
    logging.debug("Waiting for "+str(wait_before_start)+" seconds")
    sleep(wait_before_start)
    logging.debug("Starting Hadoop")
    cluster.cleanHadoopDataAtFirst()
    cluster.startHadoopMaster()
    cluster.startHadoopWorker()
  
  #click.echo(cluster.getReport())

@mastercommand.command("kafkafailure",help="Failure Simulation for Kafka")
@click.option('--failure-interval',               default=180,  help="Time interval (in seconds) to fail server")
@click.option('--wait-before-start',              default=180,  help="Time to wait (in seconds) before starting server")
@click.option('--servers',                        default="",   help="Servers to effect.  Command separated list (i.e. --servers zk1,zk2,zk3)")
@click.option('--min-servers',                    default=1,    help="Minimum number of servers that must stay up")
@click.option('--servers-to-fail-simultaneously', default=1,    help="Number of servers to kill simultaneously")
@click.option('--kill-method',                    default='kill', type=click.Choice(['restart', 'kill', "random"]), help="Server kill method.  Restart is clean, kill uses kill -9, random switches randomly")
@click.option('--initial-clean',                  is_flag=True, help="If enabled, will run a clean operation before starting the failure simulation")
@click.option('--server-config',     default='/opt/kafka/config/default.properties', help='Kafka server configuration template path, default is /opt/kafka/config/default.properties', type=click.Path(exists=True))
@click.option('--use-spare',                      is_flag=True, help="If enabled, will select spare-kafka server to run in the cluster")
@click.option('--junit-report',                   default="",   help="If set, will write the junit-report to the specified file")
@click.option('--restart-method',                 default='random', type=click.Choice(["flipflop", "random"]), help="Server restart method. 'flipflop' - it starts spare node if normal node killed and vise versa, 'random' - it starts any random node when failure occurs")
def kafkafailure(failure_interval, wait_before_start, servers, min_servers, servers_to_fail_simultaneously, kill_method, initial_clean, server_config, use_spare, junit_report, restart_method):
  global _jobs
  
  kf = KafkaFailure()
  
  p = multiprocessing.Process(name="KafkaFailure",
                              target=kf.failureSimulation, 
                              args=(failure_interval, wait_before_start, servers, min_servers, 
                                    servers_to_fail_simultaneously, kill_method, initial_clean, server_config, use_spare, junit_report, restart_method))
  _jobs.append(p)
  p.start()
  

@mastercommand.command("zookeeperfailure",help="Failure Simulation for Zookeeper")
@click.option('--failure-interval',               default=180,  help="Time interval (in seconds) to fail server")
@click.option('--wait-before-start',              default=180,  help="Time to wait (in seconds) before starting server")
@click.option('--servers',                        default="",   help="Servers to effect.  Command separated list (i.e. --servers zk1,zk2,zk3)")
@click.option('--min-servers',                    default=1,    help="Minimum number of servers that must stay up")
@click.option('--servers-to-fail-simultaneously', default=1,    help="Number of servers to kill simultaneously")
@click.option('--kill-method',                    default='kill', type=click.Choice(['restart', 'kill', "random"]), help="Server kill method.  Restart is clean, kill uses kill -9, random switches randomly")
@click.option('--initial-clean',                  is_flag=True, help="If enabled, will run a clean operation before starting the failure simulation")
@click.option('--zoo-cfg',                        default='/opt/zookeeper/conf/zoo_sample.cfg', help='Zookeeper configuration template path, default is /opt/zookeeper/conf/zoo_sample.cfg', type=click.Path(exists=True))
@click.option('--junit-report',                   default="",    help="If set, will write the junit-report to the specified file")
@click.option('--restart-method',                 default='random', type=click.Choice(["random"]), help="Server restart method. 'random' - it starts any random node when failure occurs")
def zookeeperfailure(failure_interval, wait_before_start, servers, min_servers, servers_to_fail_simultaneously, kill_method, initial_clean, zoo_cfg, junit_report, restart_method):
  global _jobs
  
  zf = ZookeeperFailure()
  p = multiprocessing.Process(name="ZookeeperFailure",
                              target=zf.failureSimulation, 
                              args=(failure_interval, wait_before_start, servers, min_servers, 
                                    servers_to_fail_simultaneously, kill_method, initial_clean, zoo_cfg, None, junit_report, restart_method))
  _jobs.append(p)
  p.start()
  

@mastercommand.command("dataflowfailure",help="Failure Simulation for dataflow")
@click.option('--role',                           default='dataflow-worker', type=click.Choice(['dataflow-worker', 'dataflow-master']), help="'dataflow-worker' will run failure simulation on dataflow-worker, 'dataflow-master' will run failure simulation on dataflow-master,")
@click.option('--failure-interval',               default=180,  help="Time interval (in seconds) to fail server")
@click.option('--junit-report',                   default="",    help="If set, will write the junit-report to the specified file")
def dataflowfailure(role, failure_interval, junit_report):
  global _jobs
  
  dataflowFailure = DataFlowFailure(role)
  p = multiprocessing.Process(name="DataflowFailure",
                              target=dataflowFailure.dataflowFailureSimulation, 
                              args=(failure_interval, junit_report))
  _jobs.append(p)
  p.start()
  


@mastercommand.command("monitor",help="Monitor Cluster status")
@click.option('--update-interval', default=30, help="Time interval (in seconds) to wait between updating cluster status")
def monitor(update_interval):
  """
  Prints the cluster report
  """
  global _jobs
  p = multiprocessing.Process(name="Monitor",
                              target=doMonitor, 
                              args=(update_interval,))
  _jobs.append(p)
  p.start()

def doMonitor(interval):
  while True:
    try:
      cluster = Cluster()
      click.echo(cluster.getReport())
      click.echo("\n\n")
    except:
      click.echo("Error getting cluster report")
    sleep(interval)
    
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
  
  for job in _jobs:
    job.join()
