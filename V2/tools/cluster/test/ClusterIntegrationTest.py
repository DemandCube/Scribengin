import unittest
from time import sleep

#Add the directory above to the python path
import sys; import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import Cluster #@UnresolvedImport


class ClusterIntegrationTest(unittest.TestCase):
  """
  This integration test requires that the Scribengin cluster is up and running
  nosetests should not run this class automatically
  """
  def test_LaunchCluster(self):
    cluster = Cluster.Cluster()
    
    cluster.report()
    print "\n"
    cluster.reportProcess("kafka")
    print "\n"
    cluster.reportProcess("zookeeper")
    print "\n"
    
    
    print "Clean ZK and Kafka"
    cluster.cleanZookeeper()
    cluster.cleanKafka()
    
    
    print "Start ZK"
    cluster.startZookeepers()
    print "Start Kafka"
    cluster.startKafka()
    sleep(5)
    
    cluster.report()
    print "\n"
    cluster.reportProcess("kafka")
    print "\n"
    cluster.reportProcess("zookeeper")
    print "\n"
    
    self.assertTrue(cluster.isProcessRunning("zookeeper"))
    self.assertTrue(cluster.isProcessRunning("kafka"))
    
    cluster.report()
    print "\n"
    cluster.reportProcess("kafka")
    print "\n"
    cluster.reportProcess("zookeeper")
    print "\n"
    
    print "Shutdown ZK"
    cluster.shutdownZookeepers()
    print "Shutdown Kafka"
    cluster.shutdownKafka()
    sleep(5)
    
    #Sometimes Kafka/ZK likes to stay up and running
    if(cluster.isProcessRunning("zookeeper")):
      cluster.killZookeepers()
    if(cluster.isProcessRunning("kafka")):
      cluster.killKafka()
    self.assertFalse(cluster.isProcessRunning("kafka"))
    cluster.cleanZookeeper()
    cluster.cleanKafka()
    
    cluster.report()
    print "\n"
    cluster.reportProcess("kafka")
    print "\n"
    cluster.reportProcess("zookeeper")
    print "\n"
    
    
    
    print "Start ZK"
    cluster.startZookeepers()
    print "Start Kafka"
    cluster.startKafka()
    sleep(5)
    self.assertTrue(cluster.isProcessRunning("zookeeper"))
    self.assertTrue(cluster.isProcessRunning("kafka"))
    
    print "Kill ZK"
    cluster.killZookeepers()
    print "Kill Kafka"
    cluster.killKafka()
    self.assertFalse(cluster.isProcessRunning("zookeeper"))
    self.assertFalse(cluster.isProcessRunning("kafka"))
    
    
    
    #TODO - expand for Hadoop
    
    
    
    
if __name__ == '__main__':
  unittest.main()