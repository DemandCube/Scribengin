import unittest
from time import sleep
from .. import Cluster 


class ClusterIntegrationTest(unittest.TestCase):
  """
  This integration test requires that the Scribengin cluster is up and running
  nosetests should not run this class automatically
  """
  def test_LaunchCluster(self):
    x = Cluster.Cluster()
    x.report()
    
    print "Clean ZK and Kafka"
    x.cleanZookeeper()
    x.cleanKafka()
    
    print "Start ZK"
    x.startZookeepers()
    print "Start Kafka"
    x.startKafka()
    sleep(5)
    self.assertTrue(x.isProcessRunning("zookeeper"))
    self.assertTrue(x.isProcessRunning("kafka"))
    
    x.reportProcess("kafka")
    x.reportProcess("zookeeper")
    
    
    print "Shutdown ZK"
    x.shutdownZookeepers()
    print "Shutdown Kafka"
    x.shutdownKafka()
    sleep(5)
    
    #Sometimes Kafka/ZK likes to stay up and running
    if(x.isProcessRunning("zookeeper")):
      x.killZookeepers()
    if(x.isProcessRunning("kafka")):
      x.killKafka()
    self.assertFalse(x.isProcessRunning("kafka"))
    x.cleanZookeeper()
    x.cleanKafka()
    
    print "Start ZK"
    x.startZookeepers()
    print "Start Kafka"
    x.startKafka()
    sleep(5)
    self.assertTrue(x.isProcessRunning("zookeeper"))
    self.assertTrue(x.isProcessRunning("kafka"))
    
    print "Kill ZK"
    x.killZookeepers()
    print "Kill Kafka"
    x.killKafka()
    self.assertFalse(x.isProcessRunning("zookeeper"))
    self.assertFalse(x.isProcessRunning("kafka"))
    
    
    
    #TODO - expand for Hadoop
    
    
    
    
if __name__ == '__main__':
  unittest.main()