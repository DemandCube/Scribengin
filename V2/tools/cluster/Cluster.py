from Server import KafkaServer,ZookeeperServer,HadoopMasterServer,HadoopWorkerServer
from ServerSet import ServerSet

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
    f = open(path, 'r')
    for line in f:
      if any(regex.match(line) for regex in self.serverRegexes):
        hostname = line.rstrip().split()[1]
        if re.match("kafka.*", hostname, re.IGNORECASE) is not None:
          self.addServer(KafkaServer(hostname))
        if re.match("zookeeper.*", hostname, re.IGNORECASE) is not None:
          self.addServer(ZookeeperServer(hostname)) 
        if re.match("hadoop-master.*", hostname, re.IGNORECASE) is not None:
          self.addServer(HadoopMasterServer(hostname)) 
        if re.match("hadoop-worker.*", hostname, re.IGNORECASE) is not None :
          self.addServer(HadoopWorkerServer(hostname))
  
  
  def getServersByRole(self, role):
    """
    Returns a subset of the ServerSet that has the role passed in
    """
    serverSet = ServerSet(role);
    for server in self.servers :
      if(server.getRole() == role) :
        serverSet.addServer(server);
    return serverSet;
  
  def startZookeepers(self):
    return self.startProcess("zookeeper")
  
  def startKafka(self):
    return self.startProcess("kafka")
  
  def startHadoopMaster(self):
    return self.startProcess("hadoop-master")
  
  def startHadoopWorker(self):
    return self.startProcess("hadoop-worker")
  
  def shutdownZookeepers(self):
    return self.shutdownProcess("zookeeper")
  
  def shutdownKafka(self):
    return self.shutdownProcess("kafka")
  
  def shutdownHadoopMaster(self):
    return self.shutdownProcess("hadoop-master")
  
  def shutdownHadoopWorker(self):
    return self.shutdownProcess("hadoop-worker")
    
  def killZookeepers(self):
    return self.killProcess("zookeeper")
  
  def killKafka(self):
    return self.killProcess("kafka")
  
  def killHadoopMaster(self):
    return self.killProcess("hadoop-master")
  
  def killHadoopWorker(self):
    return self.killProcess("hadoop-worker")  
  
  def cleanKafka(self):
    return self.cleanProcess("kafka")
  
  def cleanZookeeper(self):
    return self.cleanProcess("zookeeper")
  
  def cleanHadoopMaster(self):
    return self.cleanProcess("hadoop-master")
  
  def cleanHadoopWorker(self):
    return self.cleanProcess("hadoop-worker")
    
  def report(self) : 
    """
    Prints out report
    """
    for server in self.servers :
      server.report();
