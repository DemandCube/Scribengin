'''
Created on Mar 28, 2015

@author: Tuan
'''
class Process(object):
    def __init__(self, name, hostname, homeDir, processIdentifier):
        self.name = name ;
        self.hostname = hostname;
        self.homeDir = homeDir;
        self.processIdentifier = processIdentifier;
        
        
    def getName(self):
        return self.name
    
    def getRunningPid(self):
        return -1;
    
    def kill(self):
        print("Get the pid and kill the process");

class KafkaProcess(Process):
    def __init__(self, hostname):
        Process.__init__(self, "Kafka", hostname, "/opt/kafka", "Kafka Identifier")

    def start(self):
        print("TODO: call $homeDir/bin/start.sh");
        
    def shutdown(self):
        print("TODO: call $homeDir/bin/shutdown.sh");

class ZookeeperProcess(Process):
    def __init__(self, hostname):
        Process.__init__(self, 'Zookeeper',hostname, "/opt/zookeeper", 'Zookeeper Identifier')

    def shutdown(self):
        print("TODO: call $homeDir/bin/shutdown.sh");
