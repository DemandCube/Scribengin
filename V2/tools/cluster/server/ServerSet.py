'''
Created on Mar 28, 2015

@author: Tuan
'''

class ServerSet(object):
    def __init__(self, name):
        self.name = name ;
        self.servers = [];
    
    def clear(self):
        self.servers = [];
        
    def addServer(self, server):
        self.servers.append(server);
    
    def startProcess(self, processName):
        for server in self.servers :
            process = server.getProcess(processName) ;
            process.start() ;
    
    def killProcess(self, processName):
        for server in self.servers :
            process = server.getProcess(processName) ;
            process.kill() ;
    
    def shutdownProcess(self, processName):
        for server in self.servers :
            process = server.getProcess(processName) ;
            process.shutdown() ;
    
    def getServers(self):
        return self.servers;
    
    def report(self) : 
        for server in self.servers :
            server.report();

    def reportProcess(self, processName) : 
        for server in self.servers :
            print("TODO: print out in the table format server , process, running status");
