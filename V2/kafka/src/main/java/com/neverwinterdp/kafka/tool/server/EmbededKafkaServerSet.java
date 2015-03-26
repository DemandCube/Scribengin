package com.neverwinterdp.kafka.tool.server;

import java.util.Map;

import com.neverwinterdp.tool.server.ServerSet;

public class EmbededKafkaServerSet extends ServerSet<EmbededKafkaServer> {
  private int numOfPartitions  = 1;
  private int replication      = 1;
  private boolean verbose = true;
  
  public EmbededKafkaServerSet(String baseDir, int basePort, int numOfServers, Map<String, String> serverProps) {
    super("kafka", baseDir, basePort, numOfServers, serverProps);
  }

  public EmbededKafkaServerSet setVerbose(boolean b) {
    this.verbose = b;
    return this;
  }
  
  public EmbededKafkaServerSet setNumOfPartition(int number) {
    this.numOfPartitions = number;
    return this;
  }
  
  public EmbededKafkaServerSet setReplication(int replication) {
    this.replication = replication;
    return this;
  }
  
  @Override
  protected EmbededKafkaServer newServer(int id, String serverName, String serverDir, int serverPort, Map<String, String> props) {
    EmbededKafkaServer server = new EmbededKafkaServer(id, serverDir, serverPort);
    server.setVerbose(verbose);
    server.setReplication(replication);
    server.setNumOfPartition(numOfPartitions);
    return server;
  }

}
