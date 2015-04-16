from tabulate import tabulate
from sys import path
from os.path import join, dirname, abspath, expanduser
#Make sure the cluster package is on the path correctly
path.insert(0, dirname(dirname(abspath(__file__))))
from process.Process import KafkaProcess,ZookeeperProcess,HadoopDaemonProcess, VmMasterProcess, ScribenginProcess  #@UnresolvedImport
from yarnRestApi.YarnRestApi import YarnRestApi #@UnresolvedImport

class Server(object):
  def __init__(self, hostname, sshKeyPath=join(expanduser("~"),".ssh/id_rsa")):
    self.hostname = hostname 
    self.role = 'unknown' 
    self.processes = {} 
    self.sshKeyPath=sshKeyPath
        
  def getHostname(self):
    return self.hostname;
    
  def getRole(self):
    return self.role ;
    
  def addProcess(self, process):
    self.processes[process.getRole()] =  process
    
  def getProcess(self, name):
    return self.processes.get(name, None)
    
  def getProcesses(self):
    return self.processes
  
  def startProcess(self, processName, paramDict = {}, setupClusterEnv = True):
    for key in self.processes:
      if processName == self.processes[key].getRole():
        if setupClusterEnv:
          self.processes[key].setupClusterEnv(paramDict)
        self.processes[key].start()
  
  def shutdownProcess(self, processName):
    for key in self.processes:
      if processName == self.processes[key].getRole():
        self.processes[key].shutdown()
  
  def cleanProcess(self, processName):
    for key in self.processes:
      if processName == self.processes[key].getRole():
        self.processes[key].clean()
  
  def killProcess(self, processName):
    for key in self.processes:
      if processName == self.processes[key].getRole():
        self.processes[key].kill()
  
  def startProcesses(self,  paramDict = {}):
    for key in self.processes:
      self.processes[key].start(paramDict)
  
  def shutdownProcesses(self):
    for key in self.processes:
      self.processes[key].shutdown()
  
  def killProcesses(self):
    for key in self.processes:
      self.processes[key].kill()
  
  def sshExecute(self, command):
      self.processes.values()[0].sshExecute(command)
      
  def getReportDict(self):
    procDict= {}
    for key in self.processes:
      running = "Running"
      if not self.processes[key].isRunning():
        running = "None"  
      procDict["Status"] = running
      
    return dict({
            "Hostname" : self.hostname,
            "Role"     : self.role,
            "Num processess": len(self.processes.keys())}.items() + procDict.items())
                
    
  def report(self):
    report = self.getReportDict()
    print tabulate([report.values()] )

  
class KafkaServer(Server):
  def __init__(self, hostname):
    Server.__init__(self, hostname)
    self.role = 'kafka' 
    Server.addProcess(self, KafkaProcess(hostname))

class ZookeeperServer(Server):
  def __init__(self, hostname):
    Server.__init__(self, hostname)
    self.role = 'zookeeper'
    Server.addProcess(self, ZookeeperProcess(hostname))
        
class HadoopWorkerServer(Server):
  def __init__(self, hostname):
    Server.__init__(self, hostname)
    self.role = 'hadoop-worker' 
    Server.addProcess(self, HadoopDaemonProcess('datanode',hostname, 'DataNode', "sbin/hadoop-daemon.sh"))
    Server.addProcess(self, HadoopDaemonProcess('nodemanager',hostname, 'NodeManager', "sbin/yarn-daemon.sh"))

class HadoopMasterServer(Server):
  def __init__(self, hostname):
    Server.__init__(self, hostname)
    self.role = 'hadoop-master'
    Server.addProcess(self, HadoopDaemonProcess('namenode',hostname, 'NameNode', "sbin/hadoop-daemon.sh"))
    Server.addProcess(self, HadoopDaemonProcess('secondarynamenode',hostname, 'SecondaryNameNode', "sbin/hadoop-daemon.sh"))
    Server.addProcess(self, HadoopDaemonProcess('resourcemanager',hostname, 'ResourceManager', "sbin/yarn-daemon.sh"))
    
class VmMasterServer(Server):
  def __init__(self, hostname):
    Server.__init__(self, hostname)
    self.role = 'vmMaster'
    Server.addProcess(self, VmMasterProcess('vmmaster',hostname))
  
  def getReportDict(self):
    runningOn = ""
    yarnConnection = YarnRestApi(self.hostname)
    runningAppMasters = yarnConnection.getRunningApplicationMasters()
    if runningAppMasters and "apps" in runningAppMasters and "app" in runningAppMasters["apps"]:
      for appMaster in runningAppMasters["apps"]["app"]:
        if "amHostHttpAddress" in appMaster:
          runningOn = appMaster["amHostHttpAddress"]
    
      runningOn = runningOn.split(":")[0]
      procDict = super(VmMasterServer, self).getReportDict()
      procDict["Hostname"] = runningOn
      return procDict
    else:
      return None
  
class ScribenginServer(Server):
  def __init__(self, hostname):
    Server.__init__(self, hostname)
    self.hostname = hostname
    self.role = 'scribengin'
    Server.addProcess(self, ScribenginProcess('scribengin',hostname))
  
  def getReportDict(self):
    for key in self.processes:
      if not self.processes[key].isRunning():
        return None
    return super(ScribenginServer, self).getReportDict()
