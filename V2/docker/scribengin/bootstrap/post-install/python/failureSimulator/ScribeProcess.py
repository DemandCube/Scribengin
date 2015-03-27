from ScribeServer import ScribeServer


class ScribeProcess:
  def __init__(self, scribeServerHelper=ScribeServer()):
    self.scribeServerHelper = scribeServerHelper
  
  def getRunningKafkaProcessIDs(self):
    return self.getRunningProcessIDs("kafka")
  
  def getRunningZookeeperProcessIDs(self):
    return self.getRunningProcessIDs("QuorumPeerMain")
  
  def getRunningProcessIDs(self, procName):
    command = "ps ax | grep -i '"+procName+"' | grep java | grep -v grep | awk '{print $1}'"
    output = self.scribeServerHelper.execServer(command, "kafka")
    result = []
    for o in output:
      result.append(output[o]["stdout"].strip())
      
    return result
  
  
  
  
  