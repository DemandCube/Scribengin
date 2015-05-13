from sys import exit
from time import sleep,time
from random import randint, sample
from junit_xml import TestSuite, TestCase
import logging
import os, re

from Cluster import Cluster  #@UnresolvedImport

class FailureSimulator():
  def __init__(self, role=None):
    self.roleName = role
    self.mainCluster = Cluster()
    self.spareCluster = None
    self.cluster = None
    
  def isSpareNode(self, hostname):
    if re.match('.*spare-.*', hostname):
      return True
    else:
      return False
  
  def getRoleName(self, hostname):
    if(self.isSpareNode(hostname)):
      return "spare-"+self.roleName
    else:
      return self.roleName
  
  def getExecutionCluster(self, hostname):
    if(self.isSpareNode(hostname)):
      return self.spareCluster
    else:
      return self.cluster
    
  def failureSimulation(self,failure_interval, wait_before_start, servers, min_servers, servers_to_fail_simultaneously, kill_method, initial_clean, config_path, use_spare, junit_report, restart_method):
    """
    Run the failure loop for a given role
    """
    logging.debug("Failure interval: " + str(failure_interval))
    logging.debug("Wait before start: " + str(wait_before_start)) 
    logging.debug("Minimum number of servers: " + str(min_servers))
    logging.debug("Number of servers to fail simultaneously: " + str(servers_to_fail_simultaneously)) 
    logging.debug("Kill method: " + kill_method)
    logging.debug("Initial clean: " + str(initial_clean)) 
    logging.debug("Role name: "+ self.roleName)
    logging.debug("Junit Report: "+ junit_report)
    
    testCases = []
    testNum = 0
    spareRoleName = "spare-"+self.roleName
    
    ''' 
    Set config path
    '''
    if self.roleName == "kafka":
      self.mainCluster.paramDict["server_config"] = config_path
    elif self.roleName == "zookeeper":
      self.mainCluster.paramDict["zoo_cfg"] = config_path
        
    serverArray = []
    if(servers == ""):
      self.cluster = self.mainCluster.getServersByRole(self.roleName)
      for server in self.cluster.servers:
        serverArray.append(server.getHostname())
      logging.debug("Server list: " + ",".join(serverArray))
    else:
      serverArray = servers.split(",")
      self.cluster = self.mainCluster.getServersByHostname(serverArray)
      logging.debug("Server list: " + servers)
      
    if min_servers >= self.cluster.getNumServers():
      raise ValueError("Minimum Number of servers is too high!\nMinimum Servers to stay up: "
                       +str(min_servers)+"\nNumber of "+self.roleName+" servers in cluster: "+str(self.cluster.getNumServers()))
      exit(-1)
    
    if servers_to_fail_simultaneously > self.cluster.getNumServers() - min_servers:
      raise ValueError("--servers_to_fail_simultaneously is set too high")
      exit(-1)   
    
    spareServersArray = []
    if use_spare:
      self.spareCluster = self.mainCluster.getServersByRole(spareRoleName)
      for server in self.spareCluster.servers:
        spareServersArray.append(server.getHostname())
    
    if initial_clean:
      self.cluster.cleanProcess(self.roleName)
      if use_spare:
        self.spareCluster.cleanProcess(spareRoleName)
    
    
    #Find running and Idle servers initially    
    runningServers = []
    idleServers = []
    for server in self.cluster.servers + self.spareCluster.servers:
      hostname = server.getHostname()
      if server.getProcess(self.getRoleName(hostname)).isRunning():
        runningServers.append(hostname)
      else:
        idleServers.append(hostname)

    while True:
      logging.debug("Running servers before kill/shutdown: " + str(runningServers))
      logging.debug("Idle servers before kill/shutdown: "+str(idleServers))
      start = time()
      logging.debug("Sleeping for "+str(failure_interval)+" seconds")
      sleep(failure_interval)
      
      serversToStart = []
      #pick random servers to kill
      serversToKill = sample(runningServers, servers_to_fail_simultaneously)
      logging.debug("Servers selected to kill: "+ ','.join(serversToKill))
      
      #assigning servers to start with servers to kill
      serversToStart = serversToKill[:]
      
      #Stop the running process based on kill_method
      currentExecutingCluster = None
      for hostname in serversToKill:
        roleName = self.getRoleName(hostname)
        currentExecutingCluster = self.getExecutionCluster(hostname)
          
        if kill_method == "restart" :
          #Shutting down process
          currentExecutingCluster.shutdownProcessOnHost(roleName, hostname)
        elif kill_method == "kill":
          #Killing process
          currentExecutingCluster.killProcessOnHost(roleName, hostname)
        else:
          if randint(0,1) == 0:
            #Shutting down process
            currentExecutingCluster.shutdownProcessOnHost(roleName, hostname)
          else:
            #Killing process
            currentExecutingCluster.killProcessOnHost(roleName, hostname)
        
      #Ensure the process has stopped
      for hostname in serversToKill:
        roleName = self.getRoleName(hostname)
        currentExecutingCluster = self.getExecutionCluster(hostname)
          
        #Create basis for test case
        tc = TestCase('Test'+str(testNum), roleName+'FailureSimulator', time()-start, 
                      'Shutting down '+roleName+" with kill_method "+kill_method+" on host "+hostname, '')
        #If the process is still running, then try killing it one more time
        if(currentExecutingCluster.isProcessRunningOnHost(roleName, hostname)):
          logging.debug("Killing "+roleName + " on " +hostname+" one last time")
          currentExecutingCluster.killProcessOnHost(roleName, hostname)
          #If the process is *still* running then report a failure
          if(currentExecutingCluster.isProcessRunningOnHost(roleName, hostname)):
            tc.add_failure_info(roleName+" process is still running on"+hostname, "")
          else:
            #update running and idle server list
            runningServers.remove(hostname)
            idleServers.append(hostname)
        else:
          #update running and idle server list
          runningServers.remove(hostname)
          idleServers.append(hostname)
          
        testCases.append(tc)
        testNum+=1
        
      logging.debug("Running servers after kill/shutdown: " + str(runningServers))
      logging.debug("Idle servers after kill/shutdown: "+str(idleServers))
      
      #Start the process again
      start = time()
      sleep(wait_before_start)
      newServers = []
      
      if use_spare:
        serversToStart = []
        tempList = idleServers[:]
        for hostname in serversToKill:
          if restart_method == "flipflop":
            if self.isSpareNode(hostname):
              filtered_list = filter(lambda i: not re.match('.*spare-.*', i), tempList)
            else:
              filtered_list = filter(lambda i: re.match('.*spare-.*', i), tempList)
            
            if not filtered_list:
              tempHostname = sample(tempList,1)
            else:
              tempHostname =  sample(filtered_list,1)
          else:
            tempHostname = sample(tempList, 1)
          serversToStart.append(tempHostname[0])
          tempList.remove(tempHostname[0])
        
      for hostname in serversToStart:
        setupClusterEnv = False
        if hostname not in serversToKill:
          newServers.append(hostname)
          currentExecutingCluster.cleanProcessOnHost(roleName, hostname)
          setupClusterEnv = True
          
        roleName = self.getRoleName(hostname)
        currentExecutingCluster = self.getExecutionCluster(hostname)
        
        #starting process
        currentExecutingCluster.startProcessOnHost(roleName, hostname, setupClusterEnv)
      
      
      #Reassign replicas processes
      if len(newServers) > 0:
        #Getting new broker list
        if self.roleName == "kafka":
          newBrokers = []
          for hostname in newServers:
            roleName = self.getRoleName(hostname)
            currentExecutingCluster = self.getExecutionCluster(hostname)
            brokerID = int(re.search(r'\d+', hostname).group())
            if roleName == "spare-kafka":
              brokerID = brokerID + len(currentExecutingCluster.paramDict["kafkaServers"])
            newBrokers.append(str(brokerID))
            
          #Select random server to run reassigning replicas script
          serverToRunReassignReplicasScript = sample(newServers, 1)
          roleName = self.getRoleName(serverToRunReassignReplicasScript[0])
          
          currentExecutingCluster = self.getExecutionCluster(serverToRunReassignReplicasScript[0])
          logging.debug("Reassigning replicas for "+",".join(newBrokers))
          #start run reassigning replicas
          currentExecutingCluster.servers[0].getProcess(roleName).reassignReplicas(currentExecutingCluster.paramDict["zkList"], ",".join(newBrokers))
        elif self.roleName == "zookeeper":
          '''
          TODO if any required
          '''
          
      #Ensure the process has started, otherwise report a failure
      for hostname in serversToStart:
        roleName = self.getRoleName(hostname)
        currentExecutingCluster = self.getExecutionCluster(hostname)
          
        tc = TestCase('Test'+str(testNum), roleName+'FailureSimulator', time()-start, 
                      'Starting '+roleName+" on host: "+hostname, '')
        if( not currentExecutingCluster.isProcessRunningOnHost(roleName, hostname)):
          tc.add_failure_info(roleName+" process is still running on"+hostname, "")
        else:
          #update running and idle server list
          runningServers.append(hostname)
          idleServers.remove(hostname)
        testCases.append(tc)
        testNum+=1
      
      logging.debug("Running servers after started: " + str(runningServers))
      logging.debug("Idle servers after started: "+str(idleServers))
      if(not junit_report == "" ):
        logging.debug("Writing junit report to: "+junit_report)
        if(not os.path.exists(os.path.dirname(junit_report))):
          os.makedirs(os.path.dirname(junit_report))
        f = open(junit_report,'w')
        ts = TestSuite(self.roleName+" Test Suite", testCases)
        f.write(TestSuite.to_xml_string([ts]))
        f.close()

class KafkaFailure(FailureSimulator):
  def __init__(self):
    FailureSimulator.__init__(self, "kafka");

class ZookeeperFailure(FailureSimulator):
  def __init__(self):
    FailureSimulator.__init__(self, "zookeeper");

class DataFlowFailure(FailureSimulator):
  def __init__(self, role):
    FailureSimulator.__init__(self, role);

  def getServerArray(self, cluster):
    serverArray = []
    for server in cluster.servers:
      serverArray.append(server.getHostname())
    return serverArray
  
  def sshExecuteOnHost(self, cluster, hostname, command):
    for server in cluster.servers:
      if server.getHostname() == hostname:
        return server.sshExecute(command, False)
  
  def getRunningDataflowProcessNamesOnHost(self, cluster, hostname, processName):
    command = "jps -m | grep '" + processName + "' | awk '{print $4}'"
    return self.sshExecuteOnHost(cluster, hostname, command)[0].strip().split("\n")

  def killProcessOnHost(self, cluster, hostname, processName):
    command = "jps -m | grep '" + processName + "' | awk '{print $1}' | xargs kill -9"
    return self.sshExecuteOnHost(cluster, hostname, command)
    
  def isProcessRunningOnHost(self, cluster, hostname, processName):
    processNames = self.getRunningDataflowProcessNamesOnHost(cluster, hostname, processName)
    if processNames[0] != '':
      return True
    else:
      return False
  
  def dataflowFailureSimulation(self, failure_interval, junit_report):
    
    testCases = []
    testNum = 0
    
    logging.debug("Failure interval: " + str(failure_interval))
    logging.debug("Role name: "+ self.roleName)
    logging.debug("Junit Report: "+ junit_report)
    
    num_process_to_fail_simultaneously = 1
    cluster = Cluster()
    cluster = cluster.getServersByRole("hadoop-worker")
    serverArray = self.getServerArray(cluster)
    
    while True:
      start = time()
      failedCount = 0
      sleep(failure_interval)
      killedProcesses = {}
      while True:
        serversToCheck = sample(serverArray, num_process_to_fail_simultaneously)
        for hostname in serversToCheck:
          processNames = self.getRunningDataflowProcessNamesOnHost(cluster, hostname, self.roleName + "-*")
          processName = sample(processNames, 1)[0]
          if processName != '':
            logging.debug("Killing " + processName + " on " + hostname)
            killedProcesses[hostname] = processName
            self.killProcessOnHost(cluster, hostname, processName)
            failedCount = failedCount+1
          if failedCount == num_process_to_fail_simultaneously:
            break
        if failedCount == num_process_to_fail_simultaneously:
          break
      
      for hostname in killedProcesses.keys():
        processName = killedProcesses[hostname]
        #Create basis for test case
        tc = TestCase('Test'+str(testNum), processName+'FailureSimulator', time()-start, 
                      'Killing '+processName+" on host "+hostname, '')
        #If the process is still running, then try killing it one more time
        if(self.isProcessRunningOnHost(cluster, hostname, processName)):
          logging.debug("Killing "+processName + " on " +hostname+" one last time")
          self.killProcessOnHost(cluster, hostname, processName)
          #If the process is *still* running then report a failure
          if(cluster.isProcessRunningOnHost(processName, hostname)):
            tc.add_failure_info(processName+" is still running on"+hostname, "")
        testCases.append(tc)
        testNum+=1
        
      if(not junit_report == "" ):
        logging.debug("Writing junit report to: "+junit_report)
        if(not os.path.exists(os.path.dirname(junit_report))):
          os.makedirs(os.path.dirname(junit_report))
        f = open(junit_report,'w')
        ts = TestSuite(self.roleName+" Test Suite", testCases)
        f.write(TestSuite.to_xml_string([ts]))
        f.close()
    