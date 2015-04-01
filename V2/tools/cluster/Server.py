from Process import KafkaProcess,ZookeeperProcess,HadoopMasterProcess,HadoopWorkerProcess
from os.path import expanduser, join
from tabulate import tabulate


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
  
  def startProcess(self, processName):
    for key in self.processes:
      if processName == self.processes[key].getRole():
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
  
  def startProcesses(self):
    for key in self.processes:
      self.processes[key].start()
  
  def shutdownProcesses(self):
    for key in self.processes:
      self.processes[key].shutdown()
  
  def killProcesses(self):
    for key in self.processes:
      self.processes[key].kill()
  
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
    Server.addProcess(self, HadoopWorkerProcess(hostname))

class HadoopMasterServer(Server):
  def __init__(self, hostname):
    Server.__init__(self, hostname)
    self.role = 'hadoop-master'
    Server.addProcess(self, HadoopMasterProcess(hostname))
    