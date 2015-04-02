package com.neverwinterdp.kafka.tool.server;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.tool.server.Server;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.zk.tool.server.EmbededZKServerSet;

public class KafkaCluster {
  private EmbededZKServerSet zkServers ;
  private EmbededKafkaServerSet kafkaServers ;

  public KafkaCluster(String serverDir) throws Exception {
    this(serverDir, 1, 3);
  }

  public KafkaCluster(String serverDir, int numOfZkInstances, int numOfKafkaInstances) throws Exception {
    FileUtil.removeIfExist(serverDir, false);
    zkServers = new EmbededZKServerSet(serverDir + "/zookeeper", 2181, numOfZkInstances);
    Map<String, String> kafkaProps = new HashMap<String, String>() ;
    kafkaServers = new EmbededKafkaServerSet(serverDir + "/kafka", 9092, numOfKafkaInstances, kafkaProps);
  }

  public KafkaCluster setVerbose(boolean b) {
    kafkaServers.setVerbose(b) ;
    return this;
  }
  
  public KafkaCluster setNumOfPartition(int number) {
    kafkaServers.setNumOfPartition(number) ;
    return this;
  }
  
  public KafkaCluster setReplication(int replication) {
    kafkaServers.setReplication(replication);
    return this;
  }

  public void start() throws Exception {
    zkServers.start();
    Thread.sleep(2000);
    kafkaServers.start();
  }

  public Server findKafkaServerByPort(int port) {
    return kafkaServers.findServerByPort(port) ;
  }
  

  public void shutdown() throws Exception {
    System.out.println("Shutdown Kafka Servers");
    System.out.println("======================");
    kafkaServers.shutdown();
    
    System.out.println("Shutdown Zookeeper Servers");
    System.out.println("==========================");
    zkServers.shutdown();
  }

  public String getZKConnect() { return zkServers.getConnectString(); }

  public String getKafkaConnect() { return kafkaServers.getConnectString(); }
}
