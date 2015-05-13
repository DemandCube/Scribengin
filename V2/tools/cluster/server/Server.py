from tabulate import tabulate
from sys import path
from os.path import join, dirname, abspath, expanduser
#Make sure the cluster package is on the path correctly
path.insert(0, dirname(dirname(abspath(__file__))))
from process.Process import KafkaProcess,ZookeeperProcess,HadoopDaemonProcess, VmMasterProcess, ScribenginMasterProcess, DataflowMasterProcess, DataflowWorkerProcess  #@UnresolvedImport
#from yarnRestApi.YarnRestApi import YarnRestApi #@UnresolvedImport

class Server(object):
  def __init__(self, hostname, role = 'unknown', sshKeyPath=join(expanduser("~"),".ssh/id_rsa")):
    self.hostname = hostname 
    self.role = role
    self.processes = {} 
    self.sshKeyPath=sshKeyPath
    self.user = "neverwinterdp"
     
  def printTitle(self, message):
    print "***********************************************************************"
    print message 
    print "***********************************************************************"  
     
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
                
  def sshExecute(self, command, enableConsoleOutput = True):
    process = self.processes.values()[0]
    output = process.sshExecute(command)
    if enableConsoleOutput:
      self.printTitle("Executing '" + command + "' on " + process.hostname)
      if not output[1]:
        print output[0]
      else:
        print output[1]
        print "***********************************************************************"
    return output
    
  def getReportDict(self):
    return dict({
            "Hostname" : self.hostname,
            "Role"     : self.role})
    
  def report(self):
    report = self.getReportDict()
    print tabulate([report.values()] )

  
class KafkaServer(Server):
  def __init__(self, hostname, role):
    Server.__init__(self, hostname, role)
    #self.role = 'kafka'
    Server.addProcess(self, KafkaProcess(role, hostname))

class ZookeeperServer(Server):
  def __init__(self, hostname, role):
    Server.__init__(self, hostname, role)
    #self.role = 'zookeeper'
    Server.addProcess(self, ZookeeperProcess(role, hostname))
        
class HadoopWorkerServer(Server):
  def __init__(self, hostname, role):
    Server.__init__(self, hostname, role)
    #self.role = 'hadoop-worker'
    Server.addProcess(self, HadoopDaemonProcess('datanode',hostname, 'DataNode', "sbin/hadoop-daemon.sh"))
    Server.addProcess(self, HadoopDaemonProcess('nodemanager',hostname, 'NodeManager', "sbin/yarn-daemon.sh"))
    Server.addProcess(self, VmMasterProcess('vmmaster',hostname))
    Server.addProcess(self, ScribenginMasterProcess('scribengin',hostname))
    Server.addProcess(self, DataflowMasterProcess('dataflow-master',hostname,'dataflow-master-*'))
    Server.addProcess(self, DataflowWorkerProcess('dataflow-worker',hostname,'dataflow-worker-*'))

class HadoopMasterServer(Server):
  def __init__(self, hostname):
    Server.__init__(self, hostname, 'hadoop-master')
    #self.role = 'hadoop-master'
    Server.addProcess(self, HadoopDaemonProcess('namenode',hostname, 'NameNode', "sbin/hadoop-daemon.sh"))
    Server.addProcess(self, HadoopDaemonProcess('secondarynamenode',hostname, 'SecondaryNameNode', "sbin/hadoop-daemon.sh"))
    Server.addProcess(self, HadoopDaemonProcess('resourcemanager',hostname, 'ResourceManager', "sbin/yarn-daemon.sh"))


    
