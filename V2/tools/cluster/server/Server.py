'''
Created on Mar 28, 2015

@author: Tuan
'''
from server import Process
class Server(object):
    def __init__(self, hostname):
        self.hostname = hostname ;
        self.role = 'unknown' ;
        self.processes = {} ;
        
    def getHostname(self):
        return self.hostname;
    
    def getRole(self):
        return self.role ;
    
    def addProcess(self, process):
        self.processes[process.getName()] =  process;
    
    def getProcess(self, name):
        return self.processes[name];
    
    def getProcesses(self):
        return self.processes;
    
    def report(self):
        print('Hostname : ' + self.hostname);
        print('  Processes: ');
        for key in self.processes:
            process = self.processes[key] ;
            print ('    ', process.getName(), 'running = false')

class KafkaServer(Server):
    def __init__(self, hostname):
        Server.__init__(self, hostname);
        self.role = 'kafka' ;
        Server.addProcess(self, Process.KafkaProcess(hostname));

class ZookeeperServer(Server):
    def __init__(self, hostname):
        Server.__init__(self, hostname);
        self.role = 'zookeeper' ;
        Server.addProcess(self, Process.ZookeeperProcess(hostname));
    