package com.neverwinterdp.zk.tool.server;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.tool.server.ServerSet;

public class EmbededZKServerSet extends ServerSet<EmbededZKServer>{

  public EmbededZKServerSet(String baseDir, int basePort, int numOfServers) {
    this(baseDir, basePort, numOfServers, new HashMap<String, String>());
  }

  public EmbededZKServerSet(String baseDir, int basePort, int numOfServers, Map<String, String> serverProps) {
    super("zookeeper", baseDir, basePort, numOfServers, serverProps);
  }
  
  @Override
  protected EmbededZKServer newServer(int id, String serverName, String serverDir, int serverPort, Map<String, String> serverProps) {
    return new EmbededZKServer(serverDir, serverPort);
  }

}
