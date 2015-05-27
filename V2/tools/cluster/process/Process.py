from os.path import expanduser, join, abspath, dirname
from sys import path
from time import sleep
from random import sample
from tabulate import tabulate
import paramiko, re, string, logging
#Make sure the cluster package is on the path correctly
path.insert(0, dirname(dirname(abspath(__file__))))

class Process(object):
  def __init__(self, role, hostname, homeDir, processIdentifier, sshKeyPath=join(expanduser("~"),".ssh/id_rsa")):
    self.role = role ;
    self.hostname = hostname;
    self.homeDir = homeDir;
    self.processIdentifier = processIdentifier;
    self.sshKeyPath = sshKeyPath
    
  def sshExecute(self, command, user = "neverwinterdp"):
    """
    SSH onto a machine, execute a command
    Returns [stdout,stderr]
    """
    key = paramiko.RSAKey.from_private_key_file(self.sshKeyPath)
    
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect( hostname = self.hostname, username = user, pkey = key, timeout = 10 )
    stdin, stdout, stderr = c.exec_command(command)
    
    stdout = stdout.read()
    stderr = stderr.read()
    c.close()
    
    return stdout,stderr
  
  def getReportDict(self):
    report = []
    running = "Running"
    if not self.isRunning():
      running = "None"
    dictionary = {
      "Role" : self.role,
      "Hostname": self.hostname,
      "HomeDir" : self.homeDir,
      "Status" : running,
      "ProcessIdentifier" : self.processIdentifier,
      "processID" : self.getRunningPid()
      }
    report.append(dictionary)
    return report
  
  def getReportDictForVMAndScribengin(self):
    report = []
    running = "Running"
    if not self.isRunning():
      running = "None"
    pids = self.getRunningPid()
    if pids != "":
      for pid in pids.split(","):
        pid_and_name = pid.split(" ")
        dictionary = {
          "Role" : self.role,
          "Hostname": self.hostname,
          "HomeDir" : self.homeDir,
          "Status" : running
          }
        
        dictionary["ProcessIdentifier"] = pid_and_name[1]
        dictionary["processID"] = pid_and_name[0]
        report.append(dictionary)
    return report
  
  def getReport(self):
    report = self.getReportDict()
    return tabulate([report.values()], headers=report.keys())
    
  def report(self):
    print self.getReport()
  
  def isRunning(self):
    return len(self.getRunningPid()) > 0
  
  def getRole(self):
    return self.role
    
  def kill(self):
    self.printProgress("Killing ")
    pids = self.getRunningPid()
    for pid in pids.split(","):
      self.sshExecute("kill -9 "+pid)
   
  def getProcessCommand(self):
    return "jps -m | grep -w '"+self.processIdentifier+"' | awk '{print $1 \" \" $2}'"
  
  def getRunningPid(self):
    command = "ps ax | grep -w '"+self.processIdentifier+"' | grep java | grep -v grep | awk '{print $1}'"
    stdout,stderr = self.sshExecute(command)
    return stdout.strip().replace("\n",",")
  
  def isDataDirExists(self):
    if self.sshExecute("if [ -d \"" + join(self.homeDir, "data") + "\" ]; then echo \"true\"; fi")[0].strip() == "true":
      return True
    else:
      return False
    
  def start(self):
    pass
  
  def shutdown(self):
    pass
  
  def clean(self):
    pass
  
  def setupClusterEnv(self, paramDict = {}):
    pass
  
  def replaceString(self, pattern, replaceStr, line):
    return re.sub(pattern, replaceStr, line.rstrip())
  
  def printProgress(self, printStr):
    print printStr + self.getRole() + " on " + self.hostname
    
############

class KafkaProcess(Process):
  def __init__(self, role, hostname):
    Process.__init__(self, role, hostname, "/opt/kafka", "Kafka")
   
  def setupClusterEnv(self, paramDict = {}):
    zkServer = paramDict["zkList"] + paramDict["spareZkList"]
    zkConnect = ":2181,".join(zkServer) + ":2181"
    brokerID = int(re.search(r'\d+', self.hostname).group())
    if self.role == "spare-kafka":
      brokerID = brokerID + len(paramDict["kafkaServers"])
    
    fileStr = ""
    for line in open(paramDict["server_config"]).readlines():
      if re.match(re.compile("broker.id=.*"), line):
        line = re.sub("broker.id=.*", "broker.id="+`brokerID`, line.rstrip()) + "\n"
      if re.match(re.compile("zookeeper.connect=.*"), line):
        line = re.sub("zookeeper.connect=.*", "zookeeper.connect="+zkConnect, line.rstrip()) + "\n"
      fileStr = fileStr + line
    return self.sshExecute("echo \"" + fileStr + "\" > " + join(self.homeDir, "config/server.properties"))
     
  def reassignReplicas(self, zkServer, new_brokers):
    logging.debug("Reassigning replicas started")
    zk_connect = ":2181,".join(zkServer) + ":2181"
    expand_json_path = ""
    topics_json_list = []
    retry = True
    while retry:
      describePath = join(self.homeDir, "bin/kafka-topics.sh --describe --zookeeper "+zk_connect)
      stdout,stderr = self.sshExecute(describePath)
      logging.debug("STDOUT from executing "+describePath+": \n"+stdout)
      logging.debug("STDERR from executing "+describePath+": \n"+stderr)
      if not stderr:
        retry = False
        new_brokers = new_brokers.split(",")
        #generating json
        topics_to_move_json = "{\\\"version\\\":1,\\\"partitions\\\":["
        for line in stdout.splitlines():
          if not re.match('.*ReplicationFactor.*', line):
            if re.match('.*Topic:.*', line):
              line = string.replace(line, "\t", " ")
              line = line + " end"
              topic= re.search("(?<=\Topic:\s)(\w+)", line).group()
              partition=re.search("(?<=\Partition:\s)(\w+)", line).group()
              replicas_list = re.compile(r'Replicas:\s*(.*?)\s*Isr:', re.DOTALL).findall(line)[0].split(",")
              isr_list = re.compile(r'Isr:\s*(.*?)\s*end', re.DOTALL).findall(line)[0].split(",")
            
              add_to_json = False
              while len(isr_list) <  len(replicas_list):
                add_to_json = True
                broker_to_add = sample(new_brokers, 1)
                if broker_to_add[0] not in isr_list:
                  isr_list.append(broker_to_add[0])
              if add_to_json:
                topics_json_list.append("{\\\"topic\\\":\\\""+str(topic)+"\\\",\\\"partition\\\":"+str(partition)+",\\\"replicas\\\":["+",".join(map(str, isr_list))+"]}")
            
        topics_to_move_json = topics_to_move_json + ",".join(topics_json_list) + "]}"
        
        print topics_to_move_json
        #Create Json file
        if len(topics_json_list) > 0:
          expand_json_path = join(self.homeDir, "expand-cluster-reassignment.json")
          stdout,stderr = self.sshExecute("echo \"" + topics_to_move_json + "\" > " + expand_json_path)
          logging.debug("STDOUT from executing "+expand_json_path+": \n"+stdout)
          logging.debug("STDERR from executing "+expand_json_path+": \n"+stderr)
          
          #execute reassignment
          executePath = join(self.homeDir, "bin/kafka-reassign-partitions.sh --zookeeper "+zk_connect+" --reassignment-json-file "+expand_json_path+" --execute") 
          stdout,stderr = self.sshExecute(executePath)
          logging.debug("STDOUT from executing "+executePath+": \n"+stdout)
          logging.debug("STDERR from executing "+executePath+": \n"+stderr)
          
          #Verify the status of the partition reassignment
          not_completed_successfully = True
          while not_completed_successfully:
            verifyPath = join(self.homeDir, "bin/kafka-reassign-partitions.sh --zookeeper "+zk_connect+" --reassignment-json-file "+expand_json_path+" --verify") 
            stdout,stderr = self.sshExecute(verifyPath)
            
            logging.debug("STDOUT from executing "+verifyPath+": \n"+stdout)
            logging.debug("STDERR from executing "+verifyPath+": \n"+stderr)
          
            for line in stdout.splitlines():
              if not re.match("Status of partition reassignment:.*", line):
                if re.match('.*completed successfully.*', line):
                  not_completed_successfully = False
                elif re.match('.*ERROR:.*', line):
                  not_completed_successfully = False
                  break
                else:
                  not_completed_successfully = True
                  break
    
            sleep(2)
          logging.debug("Reassignment Successfull....");
    
  def start(self):
    self.printProgress("Starting ")
    return self.sshExecute(join(self.homeDir, "bin/kafka-server-start.sh")+" -daemon "+ join(self.homeDir, "config/server.properties"))
        
  def shutdown(self):
    self.printProgress("Stopping ")
    return self.sshExecute( join(self.homeDir, "bin/kafka-server-stop.sh") )
  
  def clean(self):
    self.printProgress("Cleaning data of ")
    return self.sshExecute("rm -rf "+join(self.homeDir, "data")+ " && rm -rf "+join(self.homeDir, "logs"))
    
############

class ZookeeperProcess(Process):
  def __init__(self, role, hostname):
    Process.__init__(self, role, hostname, "/opt/zookeeper", 'QuorumPeerMain')
    
  def setupClusterEnv(self, paramDict = {}):
    myid_path = ""
    fileStr = ""
    hostID = int(re.search(r'\d+', self.hostname).group())
    
    if self.role == "spare-zookeeper":
      hostID = hostID + len(paramDict["zkList"])
      
    for line in open(paramDict["zoo_cfg"]).readlines():
      if re.match(re.compile("dataDir=.*"), line):
        myid_path = line.split("=")[1].replace("\n", "")
      fileStr = fileStr + line
    self.sshExecute("mkdir -p "+ myid_path +" && echo '" + `hostID` + "' > " + join(myid_path, "myid"))
    
    allZkServers = paramDict["zkList"] + paramDict["spareZkList"]
    
    for zk in allZkServers:
      zkID = int(re.search(r'\d+', zk).group())
      if re.match('.*spare-zookeeper.*', zk):
        zkID = zkID + len(paramDict["zkList"])
      line = "server."+ `zkID` + "=" + zk + ":2888:3888\n"
      fileStr = fileStr + line
    return self.sshExecute("echo '" + fileStr + "' > " + join(self.homeDir, "conf/zoo.cfg"))
    
  def start(self):
    self.printProgress("Starting ")
    return self.sshExecute("ZOO_LOG4J_PROP='INFO,ROLLINGFILE' ZOO_LOG_DIR="+join(self.homeDir,"logs")+" "+join(self.homeDir, "bin/zkServer.sh")+ " start")
    
  def shutdown(self):
    self.printProgress("Stopping ")
    return self.sshExecute( join(self.homeDir,"bin/zkServer.sh")+ " stop")
  
  def clean(self):
    self.printProgress("Cleaning data of ")
    return self.sshExecute("rm -rf "+join(self.homeDir, "data")+ " && rm -rf "+join(self.homeDir, "logs")+" && rm -rf "+ join(self.homeDir, "zookeeper.out"))

############

class HadoopDaemonProcess(Process):
  def __init__(self, role, hostname, processIdentifier, hadoopDaemonScriptPath = "unknown"):
    Process.__init__(self, role, hostname, "/opt/hadoop", processIdentifier)
    self.hadoopDaemonScriptPath = hadoopDaemonScriptPath
  
  def setupClusterEnv(self, paramDict = {}):
    slaveStr = ""
    for hWorkers in paramDict["hadoopWorkers"]:
      slaveStr = slaveStr + hWorkers + "\n"
    slavesOut = self.sshExecute("echo \"" + slaveStr + "\" > " + join(self.homeDir, "etc/hadoop/slaves"))
    
    masterStr = ""
    for hWorkers in paramDict["hadoopMasters"]:
      masterStr = masterStr + hWorkers + "\n"
    mastersOut = self.sshExecute("echo \"" + masterStr + "\" > " + join(self.homeDir, "etc/hadoop/masters"))
    
    return slavesOut + mastersOut
  
  def start(self):
    self.printProgress("Starting ")
    return self.sshExecute(join(self.homeDir, self.hadoopDaemonScriptPath) + " start " + self.getRole())
    
  def shutdown(self):
    self.printProgress("Stopping ")
    return self.sshExecute(join(self.homeDir, self.hadoopDaemonScriptPath) + " stop " + self.getRole())
  
  def clean(self):
    self.printProgress("Cleaning data of ")
    return self.sshExecute("rm -rf "+ join(self.homeDir, "data") +" && rm -rf " + join(self.homeDir, "logs") +" && "+ join(self.homeDir, "bin/hdfs") + " namenode -format") 

############
class ScribenginProcess(Process):
  def getProcessCommand(self):
    return "jps -m | grep '"+self.processIdentifier+"' | awk '{print $1 \" \" $4}'"
  
############
class ScribenginMasterProcess(ScribenginProcess):
  def __init__(self, role, hostname):
    Process.__init__(self, role, hostname, "/opt/scribengin/scribengin/bin/", "scribengin-master-*")
    self.hostname = hostname
  
  def setupClusterEnv(self, paramDict = {}):
    pass
  
  def getReportDict(self):
    return self.getReportDictForVMAndScribengin()
  
  def getRunningPid(self):
    command = "jps -m | grep '"+self.processIdentifier+"\|dataflow-master-*\|dataflow-worker-*' | awk '{print $1 \" \" $4}'"
    stdout,stderr = self.sshExecute(command)
    return stdout.strip().replace("\n",",")
  
  def start(self):
    self.printProgress("Starting ")
    stdout,stderr = self.sshExecute(join(self.homeDir, "shell.sh")+" scribengin start")
    print "STDOUT from scribengin start: \n"+stdout
    print "STDERR from scribengin start: \n"+stderr
    
  def shutdown(self):
    self.printProgress("Stopping ")
    self.sshExecute(join(self.homeDir, "shell.sh")+" scribengin shutdown")
  
  def clean(self):
    pass 
  
  def kill(self):
    return self.shutdown()

############
class VmMasterProcess(ScribenginProcess):
  def __init__(self, role, hostname):
    Process.__init__(self, role, hostname, "/opt/scribengin/scribengin/bin/", "vm-master-*")
    
  def setupClusterEnv(self, paramDict = {}):
    pass
  
  def getReportDict(self):
    return self.getReportDictForVMAndScribengin()
  
  def getRunningPid(self):
    command = "jps -m | grep '"+self.processIdentifier+"' | awk '{print $1 \" \" $4}'"
    stdout,stderr = self.sshExecute(command)
    return stdout.strip().replace("\n",",")
  
  def start(self):
    self.printProgress("Starting ")
    stdout,stderr = self.sshExecute(join(self.homeDir, "shell.sh")+" vm start")
    print "STDOUT from vm start: \n"+stdout
    print "STDERR from vm start: \n"+stderr
    
  def shutdown(self):
    self.printProgress("Stopping ")
    self.sshExecute(join(self.homeDir, "shell.sh")+" vm shutdown")
  
  def clean(self):
    pass 
  
  def kill(self):
    return self.shutdown()

############
class DataflowMasterProcess(ScribenginProcess):
  def __init__(self, role, hostname, processIdentifier):
    Process.__init__(self, role, hostname, "/opt/scribengin/scribengin/bin/", processIdentifier)
    self.hostname = hostname
    
  def setupClusterEnv(self, paramDict = {}):
    pass
  
  def getReportDict(self):
    pass
  
  def getRunningPid(self):
    pass
  
  def start(self):
    pass
    
  def shutdown(self):
    pass
  
  def clean(self):
    pass 
  
  def kill(self):
    pass

############
class DataflowWorkerProcess(ScribenginProcess):
  def __init__(self, role, hostname, processIdentifier):
    Process.__init__(self, role, hostname, "/opt/scribengin/scribengin/bin/", processIdentifier)
    self.hostname = hostname
    
  def setupClusterEnv(self, paramDict = {}):
    pass
  
  def getReportDict(self):
    pass

  def getRunningPid(self):
    pass
  
  def start(self):
    pass
    
  def shutdown(self):
    pass
  
  def clean(self):
    pass 
  
  def kill(self):
    pass
 
class ElasticSearchProcess(Process): 
  def __init__(self, role, hostname):
    Process.__init__(self, role, hostname, "/opt/scribengin/scribengin/bin/", "Elasticsearch")
  
  def setupClusterEnv(self, paramDict = {}):
    pass
  
  def getReportDict(self):
    pass

  def getRunningPid(self):
    pass
  
  def start(self):
    pass
    
  def shutdown(self):
    pass
  
  def clean(self):
    pass 
  
  def kill(self):
    pass