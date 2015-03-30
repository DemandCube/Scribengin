'''
Created on Mar 28, 2015

@author: Tuan
'''
from server import Server, ServerSet

class Cluster(ServerSet.ServerSet):
    
    def __init__(self):
        ServerSet.ServerSet.__init__(self, "cluster");
        self.parseHostFile("/etc/hosts");
        
    def parseHostFile(self, file):
        self.clear() ;
        print("TODO: parse the /etc/hosts file and populate the servers list with the possible processes according to the host name");
        self.addServer(Server.ZookeeperServer("zookeeper-1"));
        self.addServer(Server.KafkaServer("kafka-1"));
        self.addServer(Server.KafkaServer("kafka-2"));
        self.addServer(Server.KafkaServer("kafka-3"));
    
    def getServersByRole(self, role):
        serverSet = ServerSet.ServerSet(role);
        for server in self.getServers() :
            if(server.getRole() == role) :
                serverSet.addServer(server);
        return serverSet;
    
    def report(self) : 
        for server in self.servers :
            server.report();
