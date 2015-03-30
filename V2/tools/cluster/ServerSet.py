
class ServerSet(object):
  def __init__(self, name):
    self.name = name 
    self.servers = []
    
  def clear(self):
    self.servers = []
     
  def addServer(self, server):
    self.servers.append(server)
   
  def startProcess(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        process.start()
    
  def cleanProcess(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        process.clean()
  
  def shutdownProcess(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        process.shutdown()
  
  def killProcess(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        process.kill()
    
  def isProcessRunning(self, processName):
    for server in self.servers :
      process = server.getProcess(processName)
      if process is None:
        pass
      elif process.isRunning():
        return True
    return False
        
  def report(self) : 
    for server in self.servers :
      server.report()

  def getNumServers(self):
    return len(self.servers)

  def reportProcess(self, processName) : 
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        process.report()
    print("TODO: print out in the table format server , process, running status")
  
