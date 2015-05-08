#Requires:
#sudo pip install kazoo py4j
from kazoo.client import KazooClient,KazooState
import kazoo.exceptions
from os.path import basename
import logging, textwrap, json
from sys import path
from os.path import dirname, abspath

path.insert(0, dirname(dirname(abspath(__file__))))
from descriptors import DataflowDescriptor  #@UnresolvedImport



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
  
  #Converts data to json object and calls setNode
  def setNodeAsJson(self, path, data, ephemeral=False, acl=None, sequence=False):
    self.setNode(path, json.dumps(data), ephemeral, acl, sequence)
  
  #Deletes node if it exists
  def deleteNode(self, path, recursive=True):
    if self.nodeExists(path):
      return self.zk.delete(path, recursive=recursive)
    else:
      return None
  
  #Returns true if path exists, False if not
  def nodeExists(self, path):
    return self.zk.exists(path) != None
  
  #Path is the path to watch for when children are created
  #Callback is a function - with parameters path, data, and stat
  #I.E. 
  #def nodeCallback(path, data, stat):
  #  print "Node watcher"
  #  print "  Path: " + path
  #  print "  Stats: "+ str(stat)
  #  print "  Data: " + str(data)
  def watchNode(self, path, callback):
    @kazoo.client.DataWatch(self.zk, path)
    def watcher(data, stats):
      callback(path, data, stats)
    return watcher
  
  #Path is the path to watch for when children are created
  #Callback is a function - with parameters path and children
  #I.E. 
  #def childrenCallback(path, children):
  #  print path
  #  for child in children:
  #    print path+"/"+children
  #NOTE: The callback is ONLY triggered on child node creation and deletion
  #      It will NOT trigger on a data change in the children
  def watchChildren(self, path, callback):
    @kazoo.client.ChildrenWatch(self.zk, path)
    def watcher(children):
      callback(path, children)
    return watcher
  
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
    
  
#################################################

#Example callback for watchNode()
def nodeCallback(path, data, stat):
  print "Node watcher"
  print "  Path: " + path
  print "  Stats: "+ str(stat)
  print "  Data: " + str(data)
  
#Example callback for watchChildren()
def childrenCallback(path, children):
  print "Child watcher"
  for child in children:
    print "  "+path+"/"+child



#################################################


#The following is just sample code.  Assumes you have zookeeper up and running
if __name__ == '__main__':
  zkc = ZookeeperClient("zookeeper-1",2181)
  
  zkc.deleteNode("/X")
  
  zkc.watchNode("/X", nodeCallback)
  zkc.setNode("/X", "data", False,)
  assert zkc.nodeExists("/X")
  assert not zkc.nodeExists("/XXXXXX")
  zkc.setNode("/X", "data1")
  assert zkc.getData("/X") == "data1"
  
  zkc.watchChildren("/X", childrenCallback)
  zkc.setNode("/X/Y", "data2")
  zkc.setNode("/X/Z", "data3")
  zkc.deleteNode("/X/Y")
  
  zkc.setNodeAsJson("/X/Json", {u'foo': [u'bar', None, 1.0, 2]})
  
    
  desc = DataflowDescriptor.DataflowDescriptor("id", "name", "appHome", "storageDescriptor", 
                                               "sinkDescriptor", "scribe", "numWorkers", 
                                               "numExecutors", "maxExecuteTime")
  zkc.setNode("/X/DataflowDescriptor", desc.getJson())
  zkc.dump("/X", multiLine=True)
  #zkc.dump("/NeverwinterDP", multiLine=False)
  zkc.deleteNode("/X")
  
  zkc.disconnect()



