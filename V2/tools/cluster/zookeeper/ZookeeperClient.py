#Requires:
#sudo pip install kazoo
from kazoo.client import KazooClient,KazooState
import kazoo.exceptions 
from os.path import basename
import logging, textwrap



class ZookeeperClient():
  def __init__(self, hostname="127.0.0.1", port=2181, connect=True):
    logging.basicConfig()
    self.hostname = hostname
    self.port = port
    self.zk = KazooClient(hosts=hostname+":"+str(port))
    self.zk.add_listener(self.connectionListener)
    if connect:
      self.connect()
    
  
  #Connect to zookeeper
  def connect(self):
    self.zk.start()
    
  #Disconnect from zookeeper
  def disconnect(self):
    self.zk.stop()
  
  #Callback for connection status
  def connectionListener(self, state):
    if state == KazooState.LOST:
      pass
      # Register somewhere that the session was lost
      #print "Connection LOST"
    elif state == KazooState.SUSPENDED:
      pass
        # Handle being disconnected from Zookeeper
      #print "Connection SUSPENDED"
    else:
      pass
      # Handle being connected/reconnected to Zookeeper
      #print "Connection state: "+str(state)
    
  
  #Returns kazoo zk client
  def getZkClient(self):
    return self.zk
  
  #Return data at path
  def getData(self, path):
    #self.zk.get(path)[1] - Contains ZnodeStat of that node
    #Only interested in data there, so return index 0
    return self.zk.get(path)[0]
  
  #If the node doesn't exist, create it and set the data
  def setNode(self, path, data, ephemeral=False, acl=None, sequence=False):
    try:
      return self.zk.create(path, data, acl, ephemeral, sequence, True)
    except kazoo.exceptions.NodeExistsError:
      return self.zk.set(path, data)
  
  def deleteNode(self, path, recursive=True):
    if self.nodeExists(path):
      return self.zk.delete(path, recursive=recursive)
    else:
      return None
  
  #Recursively print out zookeeper tree structure
  def dump(self, path="/", indent="", multiLine=False, wrapLength=100):
    if not self.nodeExists(path):
      return
    
    data = self.zk.get(path)[0]  
    
    #Print Node data
    if data is not None and data is not "":
      #Print over multiple lines
      if multiLine:
        dataIndent = len(indent+basename(path)+" - ")
        print indent+basename(path)+" - ",
        firstPrint=True
        for line in textwrap.wrap(data, wrapLength):
          if firstPrint:
            print line
            firstPrint = False
          else:
            print (" "*dataIndent)+line
      #Print a single line, and truncate to wrapLength characters
      else:
        print indent+basename(path)+" - "+data[0:wrapLength]
    #Print Path data
    else:
      print indent+basename(path)
      
    #Dump the node's children
    children = self.zk.get_children(path)
    for child in children:
      childPath = (path+"/"+child).replace("//","/")
      self.dump(childPath, indent+"  ", multiLine, wrapLength)
    
  #Returns true if path exists, False if not
  def nodeExists(self, path):
    return self.zk.exists(path) != None
        
if __name__ == '__main__':
  zkc = ZookeeperClient("zookeeper-1",2181)
  
  #print zkc.setNode("/testing/banana/peel", "yellow", ephemeral=True)
  #print zkc.nodeExists("/testing/banana/peel")
  #print zkc.nodeExists("/testing/banana/peels")
  #print zkc.getData("/testing/banana/peel")
  zkc.dump("/NeverwinterDP", multiLine=False)
  zkc.disconnect()
