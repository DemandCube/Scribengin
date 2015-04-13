from server.Server import KafkaServer,ZookeeperServer,HadoopMasterServer,HadoopWorkerServer
from server.ServerSet import ServerSet

import re


class Cluster(ServerSet):
  """
  A class to help discover, store, sort, and communicate with Scribengin Servers
  """

  """Regexes to find servers from /etc/hosts"""
  serverRegexes = [
    re.compile('.*kafka-\d+.*'),
    re.compile('.*zookeeper-\d+.*'),
    re.compile('.*hadoop-worker-\d+.*'),
    re.compile('.*hadoop-master.*'),
  ]
  
  def __init__(self, etcHostsPath="/etc/hosts"):
    """
    Initialize and immediately parse the /etc/hosts file
    """
    ServerSet.__init__(self, "cluster");
    self.parseEtcHosts(etcHostsPath)
    
      
  
  def parseEtcHosts(self, path="/etc/hosts"):
    """
    Parses the file in path (/etc/hosts) and searches for pattern of [ip address]\s+[hostname]
    Uses serverRegexes for server name regexes
    """
    zkList = []
    hadoopWorkers = []
    hadoopMasters = []
    f = open(path, 'r')
    for line in f:
      if any(regex.match(line) for regex in self.serverRegexes):
        hostname = line.rstrip().split()[1]
        if re.match("kafka.*", hostname, re.IGNORECASE) is not None:
          self.addServer(KafkaServer(hostname))
        if re.match("zookeeper.*", hostname, re.IGNORECASE) is not None:
          zkList.append(hostname)
          self.addServer(ZookeeperServer(hostname)) 
        if re.match("hadoop-master.*", hostname, re.IGNORECASE) is not None:
          hadoopMasters.append(hostname)
          self.addServer(HadoopMasterServer(hostname)) 
        if re.match("hadoop-worker.*", hostname, re.IGNORECASE) is not None :
          hadoopWorkers.append(hostname)
          self.addServer(HadoopWorkerServer(hostname))
    self.paramDict["zkList"] = zkList
    self.paramDict["hadoopWorkers"] = hadoopWorkers
    self.paramDict["hadoopMasters"] = hadoopMasters
    
    