import unittest,re
from time import sleep
from .. import Process

class ProcessIntegrationTest(unittest.TestCase):
  """
  This integration test requires that the Scribengin cluster is up and running
  nosetests should not run this class automatically
  """
  def test_KafkaProcess(self):
    """
    Ensures kafka can be started and stopped successfully
    """
    x = Process.KafkaProcess("kafka-1")
    self.StartingStoppingKillProcesses(x)
  
  def test_ZookeeperProcess(self):
    """
    Ensures Zookeeper can be started and stopped successfully
    """
    x = Process.ZookeeperProcess("zookeeper-1")
    self.StartingStoppingKillProcesses(x)
  
  def test_HadoopWorkerProcess(self):
    """
    Ensures Hadoop worker processes can be started and stopped successfully
    """
    x = Process.HadoopWorkerProcess("hadoop-worker-1")
    #self.StartingStoppingKillProcesses(x)
    #TODO: Implement the test
  
  def test_HadoopMasterProcess(self):
    """
    Ensures Hadoop Master Processes can be started and stopped successfully
    """
    x = Process.HadoopMasterProcess("hadoop-master")
    #self.StartingStoppingKillProcesses(x)
    #TODO: Implement the test
    
  def StartingStoppingKillProcesses(self, process):
    print "Starting"
    process.start()
    print "Started: "+process.getRunningPid()
    self.assertTrue(re.match("\d+",  process.getRunningPid()) is not None)
    self.assertTrue(process.isRunning() is True)
    
    sleep(10)
    print "Clean Shutdown"
    process.shutdown()
    sleep(5)
    print "Shutdown Complete: "+process.getRunningPid()
    self.assertTrue(re.match("\d+",  process.getRunningPid()) is None)
    self.assertTrue(process.isRunning() is False)
    
    sleep(5)
    
    process.clean()
    
    print "Starting"
    process.start()
    print "Started: "+process.getRunningPid()
    self.assertTrue(re.match("\d+",  process.getRunningPid()) is not None)
    self.assertTrue(process.isRunning() is True)
    
    sleep(10)
    print "Killing"
    process.kill()
    print "Killed: "+process.getRunningPid()
    self.assertTrue(re.match("\d+",  process.getRunningPid()) is None)
    self.assertTrue(process.isRunning() is False)
    
    
  def test_NonRunningProcess(self):
    """
    Ensures a process that can't be running returns all the correct values
    """
    x = NonRunningProcess("kafka-1")
    self.assertTrue(re.match("\d+",  x.getRunningPid()) is None)
    self.assertTrue(x.isRunning() is False)

class NonRunningProcess(Process.Process):
  def __init__(self, hostname):
    Process.Process.__init__(self, "ThisWillFail", hostname, "/opt/kafka", "ThisWillFail")    
    
if __name__ == '__main__':
  unittest.main()