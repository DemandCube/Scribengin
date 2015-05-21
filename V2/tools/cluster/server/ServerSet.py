from tabulate import tabulate
from multiprocessing import Pool
import os, sys
from os.path import join, expanduser

#This function is outside the ServerSet class
#because otherwise it wouldn't be pickleable 
#http://stackoverflow.com/questions/1816958/cant-pickle-type-instancemethod-when-using-pythons-multiprocessing-pool-ma

def frameDictionary(process, processID, processIdentifier, running):
  dictionary = {
            "Role" : process.role,
            "Hostname": process.hostname,
            "HomeDir" : process.homeDir,
            "Status" : running,
            "ProcessIdentifier" : processIdentifier,
            "processID" : processID
            }
  return dictionary

def getReportOnServer(server):
  omitStoppedProcesses = ["scribengin-master-*", "vm-master-*", "dataflow-master-*", "dataflow-worker-*"]
  scribenginRoles = ["vmmaster", "scribengin", "dataflow-master", "dataflow-worker"]
  
  result = [] #list for individual process report
  report = [] #list for server report
  serverReportDict = server.getReportDict()
  if serverReportDict is not None and serverReportDict["Hostname"] :
    result.append([serverReportDict["Role"], serverReportDict["Hostname"], "", "", "",""])
    procs = server.getProcesses()
    
    #collecting process commands from all available processes from server
    processCommand = []
    for proc in procs:
      processCommand.append(procs[proc].getProcessCommand())
    
    #ssh to run process command and get pid and process name
    stdout,stderr = procs.values()[0].sshExecute(";".join(processCommand))
    
    #extract pid and process name from stdout
    runningProcesses = {}
    for line in stdout.splitlines():
      pid_and_name = line.split(" ")
      runningProcesses[pid_and_name[0]] = pid_and_name[1]
    
    #adding none running process status
    for proc in procs:
      process = procs[proc]
      if process.processIdentifier not in runningProcesses.values():
        report.append(frameDictionary(process, "", process.processIdentifier, "None"))
    
    #adding running process status
    addedPids = [] #list to keep added pid's to report
    for proc in procs:
      process = procs[proc] 
      #filter process with the same name
      filtered_dict = {k:v for (k,v) in runningProcesses.items() if process.processIdentifier in v}
      for pid in filtered_dict:
        report.append(frameDictionary(process, pid, filtered_dict[pid], "Running"))
        addedPids.append(pid)
     
    #adding scribengin related process 
    for proc in procs:
      process = procs[proc]  
      if process.role in scribenginRoles:
        for pid in runningProcesses:
          if pid not in addedPids:
            report.append(frameDictionary(process, pid, runningProcesses[pid], "Running"))
            addedPids.append(pid)
    
    for procDict in report:
      if not (procDict["Status"] == "None" and procDict["ProcessIdentifier"] in omitStoppedProcesses):
        result.append(["","",procDict["ProcessIdentifier"], procDict["processID"], procDict["HomeDir"], procDict["Status"]])
  
  return result

class ServerSet(object):
  def __init__(self, name, numProcessesForStatus=30):
    self.role = name 
    self.servers = []
    self.paramDict = {}
    self.numProcessesForStatus = numProcessesForStatus
    
  def clear(self):
    self.servers = []
  
  def printTitle(self, message):
    print "***********************************************************************"
    print message 
    print "***********************************************************************"  
  
  def addServer(self, server):
    self.servers.append(server)
  
  def sshExecute(self, command, enableConsoleOutput = True):
    output = {}
    for server in self.servers :
      output[server.getHostname()] = server.sshExecute(command)
    return output 
  
  def sync(self, hostname, src="/opt/", dst="/opt"):
    for hostmachine in self.servers :
      if hostmachine.getHostname() == hostname:
        for server in self.servers :
          if server.getHostname() != hostname:
            self.printTitle("Sync data with " + server.getHostname() + " from " + hostname)
            command = "rsync -a -r -c -P --delete " +src+ " " + server.user +"@"+ server.getHostname() + ":"+dst
            hostmachine.sshExecute(command, False)
        break
    
  def startProcessOnHost(self, processName, hostname, setupClusterEnv = False):
    for server in self.servers :
      if server.getHostname() == hostname:
        server.startProcess(processName, self.paramDict, setupClusterEnv)
    
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
  
  def startProcess(self, processNames, setupClusterEnv = True):
    for processName in processNames.split(","):
      for server in self.servers :
        server.startProcess(processName, self.paramDict, setupClusterEnv)
    
  def cleanProcess(self, processNames):
    for processName in processNames.split(","):
      for server in self.servers : 
        server.cleanProcess(processName)
  
  def shutdownProcess(self, processNames):
    for processName in processNames.split(","):
      for server in self.servers :
        server.shutdownProcess(processName)
  
  def killProcess(self, processNames):
    for processName in processNames.split(","):
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
    serverSet.paramDict = self.paramDict
    for server in self.servers :
      if(server.getRole() == role) :
        serverSet.addServer(server)
    return serverSet
  
  def getHostnames(self):
    hostnames = []
    for server in self.servers :
      hostnames.append(server.hostname)
    return hostnames
  
  #TODO: I do not think you can start vm master this way
  def startVmMaster(self):
    hadoopMasterServers = self.getServersByRole("hadoop-worker")
    if hadoopMasterServers.servers:
      return self.startProcessOnHost("vmmaster", hadoopMasterServers.servers[0].getHostname())
  
  #TODO: I do not think you can start vm master this way
  def startScribengin(self):
    hadoopMasterServers = self.getServersByRole("hadoop-worker")
    if hadoopMasterServers.servers:
      return self.startProcessOnHost("scribengin", hadoopMasterServers.servers[0].getHostname())
  
  def startZookeeper(self):
    return self.startProcess("zookeeper")
  
  def startSpareZookeeper(self):
    return self.startProcess("spare-zookeeper")
  
  def startKafka(self):
    return self.startProcess("kafka")
  
  def startSpareKafka(self):
    return self.startProcess("spare-kafka")
  
  def cleanHadoopDataAtFirst(self):
    serverSet = self.getServersByRole("hadoop-worker")
    if not serverSet.servers[0].getProcess("datanode").isDataDirExists():
      self.cleanHadoopMaster()
      self.cleanHadoopWorker()
    
  def startNameNode(self):
    return self.startProcess("namenode", setupClusterEnv = True)
  
  def startSecondaryNameNode(self):
    return self.startProcess("secondarynamenode", setupClusterEnv = False)
  
  def startResourceManager(self):
    return self.startProcess("resourcemanager", setupClusterEnv = False)
  
  def startDataNode(self):
    return self.startProcess("datanode", setupClusterEnv = True)
  
  def startNodeManager(self):
    return self.startProcess("nodemanager", setupClusterEnv = False)
  
  def startHadoopMaster(self):
    return self.startProcess("namenode,secondarynamenode,resourcemanager")
  
  def startHadoopWorker(self):
    return self.startProcess("datanode,nodemanager")
  
  def startCluster(self):
    self.startZookeeper()
    self.startKafka()
    self.cleanHadoopDataAtFirst()
    self.startHadoopMaster()
    self.startHadoopWorker()
    self.startVmMaster()
    self.startScribengin()
    
  def shutdownVmMaster(self):
    hadoopMasterServers = self.getServersByRole("hadoop-worker")
    if hadoopMasterServers.servers:
      return self.shutdownProcessOnHost("vmmaster", hadoopMasterServers.servers[0].getHostname())
  
  def shutdownScribengin(self):
    hadoopMasterServers = self.getServersByRole("hadoop-worker")
    if hadoopMasterServers.servers:
      return self.shutdownProcessOnHost("scribengin", hadoopMasterServers.servers[0].getHostname())
  
  def shutdownZookeeper(self):
    return self.shutdownProcess("zookeeper")
  
  def shutdownSpareZookeeper(self):
    return self.shutdownProcess("spare-zookeeper")
  
  def shutdownKafka(self):
    return self.shutdownProcess("kafka")
  
  def shutdownSpareKafka(self):
    return self.shutdownProcess("spare-kafka")
  
  def shutdownNameNode(self):
    return self.shutdownProcess("namenode")
  
  def shutdownSecondaryNameNode(self):
    return self.shutdownProcess("secondarynamenode")
  
  def shutdownResourceManager(self):
    return self.shutdownProcess("resourcemanager")
  
  def shutdownDataNode(self):
    return self.shutdownProcess("datanode")
  
  def shutdownNodeManager(self):
    return self.shutdownProcess("nodemanager")
  
  def shutdownHadoopMaster(self):
    return self.shutdownProcess("namenode,secondarynamenode,resourcemanager")
  
  def shutdownHadoopWorker(self):
    return self.shutdownProcess("datanode,nodemanager")
  
  def shutdownCluster(self):
    self.killScribengin()
    self.killVmMaster()
    self.shutdownKafka()
    self.shutdownSpareKafka()
    self.shutdownZookeeper()
    self.shutdownHadoopWorker()
    self.shutdownHadoopMaster()
    
  def killScribengin(self):
    return self.killProcess("scribengin")
  
  def killVmMaster(self):
    return self.killProcess("vmmaster")
  
  def killZookeeper(self):
    return self.killProcess("zookeeper")
  
  def killSpareZookeeper(self):
    return self.killProcess("spare-zookeeper")
  
  def killKafka(self):
    return self.killProcess("kafka")
  
  def killSpareKafka(self):
    return self.killProcess("spare-kafka")
  
  def killNameNode(self):
    return self.killProcess("namenode")
  
  def killSecondaryNameNode(self):
    return self.killProcess("secondarynamenode")
  
  def killResourceManager(self):
    return self.killProcess("resourcemanager")
  
  def killDataNode(self):
    return self.killProcess("datanode")
  
  def killNodeManager(self):
    return self.killProcess("nodemanager")
  
  def killHadoopMaster(self):
    return self.killProcess("namenode,secondarynamenode,resourcemanager")
  
  def killHadoopWorker(self):
    return self.killProcess("datanode,nodemanager")  
  
  def killCluster(self):
    self.shutdownScribengin()
    self.shutdownVmMaster()
    self.killKafka()
    self.killSpareKafka()
    self.killZookeeper()
    self.killHadoopWorker()
    self.killHadoopMaster()
    
  def cleanKafka(self):
    return self.cleanProcess("kafka")
  
  def cleanSpareKafka(self):
    return self.cleanProcess("spare-kafka")
  
  def cleanZookeeper(self):
    return self.cleanProcess("zookeeper")
  
  def cleanSpareZookeeper(self):
    return self.cleanProcess("spare-zookeeper")
  
  def cleanHadoopMaster(self):
    return self.cleanProcess("namenode")
  
  def cleanHadoopWorker(self):
    return self.cleanProcess("datanode")

  def cleanCluster(self):
    self.cleanKafka()
    self.cleanSpareKafka()
    self.cleanZookeeper()
    self.cleanHadoopWorker()
    self.cleanHadoopMaster()
  
  def scribenginBuild(self,with_test):
    self.printTitle("Build Scribengin")
    command = ""
    if(with_test):
      command="../gradlew clean build install release"
    else:
      command="../gradlew clean build install release -x test"
    currentWorkingDir = self.module_path()
    
    os.chdir(join(currentWorkingDir,"../../../"))
    os.system(join(os.getcwd(),command))
    
    os.chdir(join(os.getcwd(),"release"))
    os.system(join(os.getcwd(), "../../gradlew clean release"))
    os.chdir(currentWorkingDir)
    
  def scribenginDeploy(self, hostname, aws_credential_path, clean):
    self.printTitle("Deploy Scribengin")
    self.killCluster()
    if(clean):
      self.cleanCluster()
    currentWorkingDir = self.module_path()
    self.sshExecute("rm -rf /opt/scribengin")
    self.sshExecute("rm -rf /opt/cluster")
    os.chdir(join(currentWorkingDir, "../../../"))
    os.system("scp -q -o StrictHostKeyChecking=no -r "+join(os.getcwd(),"release/build/release")+" neverwinterdp@"+hostname+":/opt/scribengin")
    os.system("scp -q -o StrictHostKeyChecking=no -r "+join(os.getcwd(),"tools/cluster")+" neverwinterdp@"+hostname+":/opt/cluster")
    if aws_credential_path == "":
      aws_credential_path = join(expanduser("~"),".aws")
    #print aws_credential_path
    os.system("scp -q -o StrictHostKeyChecking=no -r "+aws_credential_path+" neverwinterdp@"+hostname+":/home/neverwinterdp")
    self.sync(hostname, src="/home/neverwinterdp/.aws", dst="/home/neverwinterdp")
    os.chdir(currentWorkingDir)
    self.sync(hostname)
    
  def getReport(self):
    serverReport = []
    asyncresults = []
    pool = Pool(processes=self.numProcessesForStatus)
    sorted_servers = sorted(self.servers, key=lambda server: server.role)
    for server in sorted_servers:
      asyncresults.append( pool.apply_async(getReportOnServer, [server])) 
    
    for async in asyncresults:
      result = async.get(timeout=30)
      if result: 
        for row in result:
          serverReport.append(row)

    headers = ["Role", "Hostname", "ProcessIdentifier", "ProcessID", "HomeDir", "Status"]
    pool.close()
    pool.join()
    return tabulate(serverReport, headers=headers)

  def report(self) :
    print self.getReport()

  def we_are_frozen(self):
      # All of the modules are built-in to the interpreter, e.g., by py2exe
      return hasattr(sys, "frozen")
  
  def module_path(self):
      encoding = sys.getfilesystemencoding()
      if self.we_are_frozen():
          return os.path.dirname(unicode(sys.executable, encoding))
      return os.path.dirname(unicode(__file__, encoding))
