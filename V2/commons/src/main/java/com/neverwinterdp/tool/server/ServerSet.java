package com.neverwinterdp.tool.server;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.util.FileUtil;

abstract public class ServerSet<T extends Server> {
  private String serverType ;
  private String baseDir ;
  private int    basePort ;
  private int    idTracker = 0;
  private int    numOfServers ;
  private Map<String, String> serverProperties = new HashMap<>() ;
  private Map<String, T> servers = new HashMap<>() ;
  
  public ServerSet(String serverType, String baseDir, int basePort, int numOfServers, Map<String, String> serverProperties) {
    this.serverType = serverType;
    this.baseDir = baseDir;
    this.basePort = basePort ;
    this.numOfServers = numOfServers;
  }
  
  public String getServerType() { return this.serverType ; }
  
  public String getBaseDir() { return baseDir; }

  public int getBasePort() { return basePort; }

  public String getConnectString() {
    StringBuilder b = new StringBuilder() ;
    for (T server : servers.values()) {
      if(b.length() > 0) b.append(",");
      b.append(server.getConnectString());
    }
    return b.toString();
  }
  
  synchronized public T getServer(String name) { return servers.get(name) ; }
  
  synchronized public T findServerByPort(int port) { 
    for (T server : servers.values()) {
      if(server.getPort() == port) return server;
    }
    return null ;
  }
  
  synchronized public T addNewServer() {
    int id = ++idTracker;
    String serverName = serverType + id ;
    String serverDir  = baseDir + "/" + serverName;
    T server = newServer(id, serverName, serverDir, basePort + (id - 1), serverProperties) ;
    servers.put(serverName, server);
    return server ;
  }
  
  synchronized public T remove(String serverName) { return servers.remove(serverName) ; }
  
  synchronized public T shutdownAndRemove(String serverName) { 
    T server = servers.remove(serverName) ;
    if(server != null) server.shutdown(); 
    return server ;
  }
  
  abstract protected T newServer(int id, String serverName, String serverDir, int serverPort, Map<String, String> props) ;

  synchronized public void clean() throws Exception {
    FileUtil.removeIfExist(baseDir, false);
  }
  
  synchronized public void start() throws Exception {
    for(int i = 0; i < numOfServers; i++) {
      addNewServer() ;
    }
    for (T server : servers.values()) {
      server.start();
    }
  }
  
  synchronized public void shutdown() throws Exception {
    for (T server : servers.values()) {
      server.shutdown();;
    }
  }
}
