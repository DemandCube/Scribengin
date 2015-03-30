import paramiko
from Process import KafkaProcess,ZookeeperProcess,HadoopMasterProcess,HadoopWorkerProcess
from os.path import expanduser, join

class Server(object):
  def __init__(self, hostname, defaultSSHKeyPath=join(expanduser("~"),".ssh/id_rsa")):
    self.hostname = hostname 
    self.role = 'unknown' 
    self.processes = {} 
    self.defaultSSHKeyPath=defaultSSHKeyPath
        
  def getHostname(self):
    return self.hostname;
    
  def getRole(self):
    return self.role ;
    
  def addProcess(self, process):
    self.processes[process.getName()] =  process
    
  def getProcess(self, name):
    return self.processes.get(name, None)
    
  def getProcesses(self):
    return self.processes
    
  def report(self):
    print('Hostname : ' + self.hostname)
    print('  Processes: ')
    for key in self.processes:
      process = self.processes[key]
      print ('    ', process.getName(), 'running = false')


  
  
  
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
    