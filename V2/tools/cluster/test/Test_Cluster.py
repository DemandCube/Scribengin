import unittest, os

from cluster.Cluster import Cluster

class Test_Cluster(unittest.TestCase):
  def test_parseEtcHost(self):
    x = Cluster(etcHostsPath=os.path.join( os.path.dirname(os.path.realpath(__file__)),"testHostsFile"))
    
    self.assertEqual(x.getNumServers(), 8, "Should parse 8 applicable host names out of the testHostsFile")

    self.assertEqual(x.getServersByRole("kafka").getNumServers(), 3)
    self.assertEqual(x.getServersByRole("zookeeper").getNumServers(), 1)
    self.assertEqual(x.getServersByRole("hadoop-master").getNumServers(), 1)
    self.assertEqual(x.getServersByRole("hadoop-worker").getNumServers(), 3)
                
    x.report()
    

    
if __name__ == '__main__':
  unittest.main()