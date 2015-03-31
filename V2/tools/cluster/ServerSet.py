from tabulate import tabulate
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
        
  def getNumServers(self):
    return len(self.servers)

  def reportProcess(self, processName) :
    procReport = [] 
    for server in self.servers :
      process = server.getProcess(processName)
      if process is not None:
        procReport.append(process.getReportDict())
    
    print tabulate(procReport, headers="keys")


  def report(self) :
    serverReport = [] 
    for server in self.servers :
      serverReport.append(server.getReportDict())
    headers = {"hostname":"hostname",
               "ProcName":"Process Name",
               "isRunning": "isProcessRunning",
               "sshKeyPath": "sshKeyPath",
               "role":"role",
               "Num processes":"Num Processes"}
    print tabulate(serverReport, headers=headers)
    