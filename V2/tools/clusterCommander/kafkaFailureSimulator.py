from sys import path, exit
from os.path import join, dirname, abspath
from time import sleep

path.insert(0, join(dirname(dirname(abspath(__file__))), "cluster"))
from Cluster import Cluster  #@UnresolvedImport


def kafkaFailure(failure_interval, wait_before_start, servers, min_servers, servers_to_fail_simultaneously, kill_method, initial_clean):
  """
  Run the Kafka failure loop
  """
  cluster = Cluster()
  cluster = cluster.getServersByHostname(servers)
  if min_servers >= cluster.getNumServers():
    raise ValueError("Minimum Number of servers is too high!")
    exit(-1)
  
  while True:
    cluster.report()
    cluster.killProcessOnHost("kafka","kafka-1")
    sleep(5)
    cluster.report()
    cluster.startProcessOnHost("kafka","kafka-1")
    sleep(5)


