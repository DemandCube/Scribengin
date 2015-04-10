from tabulate import tabulate

class ServerSet(object):
  def __init__(self, name):
    self.role = name 
    self.servers = []
    self.paramDict = {}
    
  def clear(self):
    self.servers = []
     
  def addServer(self, server):
    self.servers.append(server)
  
  def startProcessOnHost(self, processName, hostname):
    for server in self.servers :
      if server.getHostname() == hostname:
        server.startProcess(processName, self.paramDict, setupClusterEnv = False)
    
  def cleanProcessOnHost(self, processName, hostname):
    for server in self.servers :
      if server.getHostname() == hostname:
        server.cleanProcess(processName)
  
  def shutdownProcessOnHost(self, processName, hostname):
    for server in self.servers :
      if server.getHostname() == hostname:
        server.shutdownProcess(processName)
  
  def killProcessOnHost(self, processName, hostname):
    for server in self.servers :
      if server.getHostname() == hostname:
        server.killProcess(processName)
  
  def startProcess(self, processName):
    for server in self.servers :
      server.startProcess(processName, self.paramDict)
    
  def cleanProcess(self, processName):
    for server in self.servers :
      server.cleanProcess(processName)
  
  def shutdownProcess(self, processName):
    for server in self.servers :
      server.shutdownProcess(processName)
  
  def killProcess(self, processName):
    for server in self.servers :
      server.killProcess(processName)
    
  def isProcessRunning(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is None:
        pass
      elif process.isRunning():
        return True
    return False
  
  def isProcessRunningOnHost(self, processName, hostname):
    for server in self.servers :
      if server.getHostname() == hostname:
        return server.getProcess(processName).isRunning()
    return False
        
  def getNumServers(self):
    return len(self.servers)
  
  def getServersByHostname(self, hostnames):
    serverSet = ServerSet("subset")
    serverSet.paramDict = self.paramDict
    for server in self.servers :
      if(server.getHostname().strip() in hostnames) :
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
  
  def getReport(self):
    serverReport = []
    sorted_servers = sorted(self.servers, key=lambda server: server.hostname)
    for server in sorted_servers :
      serverReport.append([server.hostname, server.role, "", "", "",""])
      procs = server.getProcesses()
      for process in procs:
        procDict = server.getProcess(process).getReportDict()

        serverReport.append(["","",procDict["ProcessIdentifier"], procDict["processID"], procDict["HomeDir"], procDict["Status"]])

    headers = ["Hostname", "Role", "ProcessIdentifier", "ProcessID", "HomeDir", "Status"]

    return tabulate(serverReport, headers=headers)
 
  def report(self) :
    print self.getReport()
