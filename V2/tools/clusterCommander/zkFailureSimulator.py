from sys import path, exit
from os.path import join, dirname, abspath
from time import sleep

path.insert(0, join(dirname(dirname(abspath(__file__))), "cluster"))
from Cluster import Cluster  #@UnresolvedImport


def zkFailure(hosts, minServers, failureInterval, waitBeforeStart):
  """
  Run the Kafka failure loop
  """
  cluster = Cluster()
  cluster = cluster.getServersByHostname(hosts)
  if minServers >= cluster.getNumServers():
    raise ValueError("Minimum Number of Kafka brokers is too high!")
    exit(-1)
  
  while True:
    cluster.report()
    cluster.killProcessOnHost("kafka","kafka-1")
    sleep(5)
    cluster.report()
    cluster.startProcessOnHost("kafka","kafka-1")
    sleep(5)
