from ScribeServer import ScribeServer


class ScribeProcess:
  def __init__(self, scribeServerHelper=ScribeServer()):
    self.scribeServerHelper = scribeServerHelper
  
  def getRunningKafkaProcessIDs(self):
    return self.getRunningProcessIDs("kafka", "kafka")
  
  def getRunningZookeeperProcessIDs(self):
    return self.getRunningProcessIDs("QuorumPeerMain", "zookeeper")
  
  def getRunningProcessIDs(self, procName, hostnameRegex):
    command = "ps ax | grep -i '"+procName+"' | grep java | grep -v grep | awk '{print $1}'"
    output = self.scribeServerHelper.execServer(command, hostnameRegex)
    result = []
    for o in output:
      result.append(output[o]["stdout"].strip())
      
    return result
  
  
  
  
  