from tabulate import tabulate
class ServerSet(object):
  def __init__(self, name):
    self.name = name 
    self.servers = []
    
  def clear(self):
    self.servers = []
     
  def addServer(self, server):
    self.servers.append(server)
   
  def startProcess(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        process.start()
    
  def cleanProcess(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        process.clean()
  
  def shutdownProcess(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        process.shutdown()
  
  def killProcess(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        process.kill()
    
  def isProcessRunning(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is None:
        pass
      elif process.isRunning():
        return True
    return False
        
  def getNumServers(self):
    return len(self.servers)
  
  def getServersByHostname(self, hostnames):
    serverSet = ServerSet("subset")
    for server in self.servers :
      if(server.getHostname() in hostnames) :
        serverSet.addServer(server)
    return serverSet
  
  def getServersByRole(self, role):
    """
    Returns a subset of the ServerSet that has the role passed in
    """
    serverSet = ServerSet(role)
    for server in self.servers :
      if(server.getRole() == role) :
        serverSet.addServer(server)
    return serverSet
  
  def startZookeeper(self):
    return self.startProcess("zookeeper")
  
  def startKafka(self):
    return self.startProcess("kafka")
  
  def startHadoopMaster(self):
    return self.startProcess("hadoop-master")
  
  def startHadoopWorker(self):
    return self.startProcess("hadoop-worker")
  
  def shutdownZookeeper(self):
    return self.shutdownProcess("zookeeper")
  
  def shutdownKafka(self):
    return self.shutdownProcess("kafka")
  
  def shutdownHadoopMaster(self):
    return self.shutdownProcess("hadoop-master")
  
  def shutdownHadoopWorker(self):
    return self.shutdownProcess("hadoop-worker")
    
  def killZookeeper(self):
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
  
  def getProcessReport(self, processName):
    procReport = [] 
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        procReport.append(process.getReportDict())
    
    return tabulate(procReport, headers="keys")
  
  def reportProcess(self, processName) :
    print self.getProcessReport(processName)

  def getReport(self):
    serverReport = [] 
    for server in self.servers :
      serverReport.append(server.getReportDict())
    headers = {"hostname":"hostname",
               "ProcName":"Process Name",
               "isRunning": "isProcessRunning",
               "sshKeyPath": "sshKeyPath",
               "role":"role",
               "Num processes":"Num Processes"}
    return tabulate(serverReport, headers=headers)
  
  def report(self) :
    print self.getReport()