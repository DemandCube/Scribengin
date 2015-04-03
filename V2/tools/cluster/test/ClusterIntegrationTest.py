import unittest
from time import sleep

from cluster import Cluster


class ClusterIntegrationTest(unittest.TestCase):
  """
  This integration test requires that the Scribengin cluster is up and running
  nosetests should not run this class automatically
  """
  def test_LaunchCluster(self):
    cluster = Cluster.Cluster()
    
    cluster.report()
    
    print "Clean ZK and Kafka"
    cluster.cleanZookeeper()
    cluster.cleanKafka()
    
    print "Start ZK"
    cluster.startZookeeper()
    print "Start Kafka"
    cluster.startKafka()
    sleep(5)
    
    cluster.report()
    
    self.assertTrue(cluster.isProcessRunning("zookeeper"))
    self.assertTrue(cluster.isProcessRunning("kafka"))
    
    cluster.report()
    
    print "Shutdown ZK"
    cluster.shutdownZookeeper()
    print "Shutdown Kafka"
    cluster.shutdownKafka()
    sleep(5)
    
    #Sometimes Kafka/ZK likes to stay up and running
    if(cluster.isProcessRunning("zookeeper")):
      cluster.killZookeeper()
    if(cluster.isProcessRunning("kafka")):
      cluster.killKafka()
    self.assertFalse(cluster.isProcessRunning("kafka"))
    cluster.cleanZookeeper()
    cluster.cleanKafka()
    
    cluster.report()
    
    print "Start ZK"
    cluster.startZookeeper()
    print "Start Kafka"
    cluster.startKafka()
    sleep(5)
    self.assertTrue(cluster.isProcessRunning("zookeeper"))
    self.assertTrue(cluster.isProcessRunning("kafka"))
    
    print "Kill ZK"
    cluster.killZookeeper()
    print "Kill Kafka"
    cluster.killKafka()
    self.assertFalse(cluster.isProcessRunning("zookeeper"))
    self.assertFalse(cluster.isProcessRunning("kafka"))
    
    
    
    #TODO - expand for Hadoop
    
    
    
    
if __name__ == '__main__':
  unittest.main()