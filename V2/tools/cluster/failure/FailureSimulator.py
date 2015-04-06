from sys import exit
from time import sleep,time
from random import randint, sample
from junit_xml import TestSuite, TestCase
import logging

from Cluster import Cluster  #@UnresolvedImport

class FailureSimulator():
  def __init__(self, role=None):
    self.roleName = role
    
  def failureSimulation(self,failure_interval, wait_before_start, servers, min_servers, servers_to_fail_simultaneously, kill_method, initial_clean, junit_report):
    """
    Run the failure loop for a given role
    """
    if(servers == ""):
      print "--servers not specified!\n\n"
      return
    
    logging.debug("Failure interval: " + str(failure_interval))
    logging.debug("Wait before start: " + str(wait_before_start)) 
    logging.debug("Server list: " + servers)
    logging.debug("Minimum number of servers: " + str(min_servers))
    logging.debug("Number of servers to fail simultaneously: " + str(servers_to_fail_simultaneously)) 
    logging.debug("Kill method: " + kill_method)
    logging.debug("Initial clean: " + str(initial_clean)) 
    logging.debug("Role name: "+ self.roleName)
    logging.debug("Junit Report: "+ junit_report)
    
    testCases = []
    testNum = 0
    
    cluster = Cluster()
    serverArray = servers.split(",")
    cluster = cluster.getServersByHostname(serverArray)
    
    if min_servers >= cluster.getNumServers():
      raise ValueError("Minimum Number of servers is too high!\nMinimum Servers to stay up: "
                       +str(min_servers)+"\nNumber of "+self.roleName+" servers in cluster: "+str(cluster.getNumServers()))
      exit(-1)
    
    if servers_to_fail_simultaneously > cluster.getNumServers() - min_servers:
      raise ValueError("--servers_to_fail_simultaneously is set too high")
      exit(-1)   
      
    if initial_clean:
      cluster.cleanProcess(self.roleName)
    
    while True:
      start = time()
      logging.debug("Sleeping for "+str(failure_interval)+" seconds")
      sleep(failure_interval)
      
      #pick random servers to kill
      serversToKill = sample(serverArray, servers_to_fail_simultaneously)
      logging.debug("Servers selected to kill: "+ ','.join(serversToKill))
      
      #Stop the running process based on kill_method
      for hostname in serversToKill:
        if kill_method == "restart" :
          logging.debug("Shutting down "+self.roleName + " on " +hostname)
          cluster.shutdownProcessOnHost(self.roleName, hostname)
        elif kill_method == "kill":
          logging.debug("Killing "+self.roleName + " on " +hostname)
          cluster.killProcessOnHost(self.roleName, hostname)
        else:
          if randint(0,1) == 0:
            logging.debug("Shutting down "+self.roleName + " on " +hostname)
            cluster.shutdownProcessOnHost(self.roleName, hostname)
          else:
            logging.debug("Killing "+self.roleName + " on " +hostname)
            cluster.killProcessOnHost(self.roleName, hostname)
      
      #Ensure the process has stopped
      for hostname in serversToKill:
        #Create basis for test case
        tc = TestCase('Test'+str(testNum), self.roleName+'FailureSimulator', time()-start, 
                      'Shutting down '+self.roleName+" with kill_method "+kill_method+" on host "+hostname, '')
        #If the process is still running, then try killing it one more time
        if(cluster.isProcessRunningOnHost(self.roleName, hostname)):
          logging.debug("Killing "+self.roleName + " on " +hostname+" one last time")
          cluster.killProcessOnHost(self.roleName, hostname)
          #If the process is *still* running then report a failure
          if(cluster.isProcessRunningOnHost(self.roleName, hostname)):
            tc.add_failure_info(self.roleName+" process is still running on"+hostname, "")
        testCases.append(tc)
        testNum+=1
        
      #Start the process again
      start = time()
      sleep(wait_before_start)
      for hostname in serversToKill:
        logging.debug("Starting " + self.roleName + " on " + hostname)
        cluster.startProcessOnHost(self.roleName, hostname)
      
      #Ensure the process has started, otherwise report a failure
      for hostname in serversToKill:
        tc = TestCase('Test'+str(testNum), self.roleName+'FailureSimulator', time()-start, 
                      'Starting '+self.roleName+" on host: "+hostname, '')
        if( not cluster.isProcessRunningOnHost(self.roleName, hostname)):
          tc.add_failure_info(self.roleName+" process is still running on"+hostname, "")
        testCases.append(tc)
        testNum+=1
      
      if(not junit_report == "" ):
        logging.debug("Writing junit report to: "+junit_report)
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

