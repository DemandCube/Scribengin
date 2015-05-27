from server.Server import KafkaServer,ZookeeperServer,HadoopMasterServer,HadoopWorkerServer,ElasticSearchServer
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
    re.compile('.*spare-kafka-\d+.*'),
    re.compile('.*spare-zookeeper-\d+.*'),
    re.compile('.*spare-hadoop-worker-\d+.*'),
    re.compile('.*elasticsearch-\d+.*'),
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
    kafkaServers = []
    spareKafkaServers = []
    zkList = []
    spareZkList = []
    hadoopWorkers = []
    spareHadoopWorkers = []
    hadoopMasters = []
    elasticsearchServers = []
    
    f = open(path, 'r')
    for line in f:
      if any(regex.match(line) for regex in self.serverRegexes):
        hostname = line.rstrip().split()[1]
        if re.match("kafka.*", hostname, re.IGNORECASE) is not None:
          self.addServer(KafkaServer(hostname, "kafka"))
          kafkaServers.append(hostname)
        if re.match("spare-kafka.*", hostname, re.IGNORECASE) is not None:
          self.addServer(KafkaServer(hostname, "spare-kafka"))
          spareKafkaServers.append(hostname)
        if re.match("zookeeper.*", hostname, re.IGNORECASE) is not None:
          zkList.append(hostname)
          self.addServer(ZookeeperServer(hostname, "zookeeper"))
        if re.match("spare-zookeeper.*", hostname, re.IGNORECASE) is not None:
          spareZkList.append(hostname)
          self.addServer(ZookeeperServer(hostname, "spare-zookeeper")) 
        if re.match("hadoop-master.*", hostname, re.IGNORECASE) is not None:
          hadoopMasters.append(hostname)
          self.addServer(HadoopMasterServer(hostname))
        if re.match("hadoop-worker.*", hostname, re.IGNORECASE) is not None :
          hadoopWorkers.append(hostname)
          self.addServer(HadoopWorkerServer(hostname, "hadoop-worker"))
        if re.match("spare-hadoop-worker.*", hostname, re.IGNORECASE) is not None :
          spareHadoopWorkers.append(hostname)
          self.addServer(HadoopWorkerServer(hostname, "spare-hadoop-worker"))
        if re.match("elasticsearch.*", hostname, re.IGNORECASE) is not None:
          self.addServer(ElasticSearchServer(hostname, "elasticsearch"))
          elasticsearchServers.append(hostname)
          
    self.paramDict["kafkaServers"] = kafkaServers
    self.paramDict["spareKafkaServers"] = spareKafkaServers
    self.paramDict["zkList"] = zkList
    self.paramDict["spareZkList"] = spareZkList
    self.paramDict["hadoopWorkers"] = hadoopWorkers
    self.paramDict["spareHadoopWorkers"] = spareHadoopWorkers
    self.paramDict["hadoopMasters"] = hadoopMasters
    self.paramDict["elasticsearchServers"] = elasticsearchServers
    self.paramDict["all"] = kafkaServers + spareKafkaServers + zkList + spareZkList + hadoopWorkers + spareHadoopWorkers + hadoopMasters + elasticsearchServers
    