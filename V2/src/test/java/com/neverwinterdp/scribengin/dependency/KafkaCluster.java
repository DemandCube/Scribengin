package com.neverwinterdp.scribengin.dependency;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import com.neverwinterdp.util.FileUtil;

public class KafkaCluster {
  private int baseZKPort = 2181;
  private int baseKafkaPort = 9092;
  private int numOfKafkaInstances = 3;
  private int numOfZkInstances = 1;
  private int numOfPartitions  = 1;
  private int replication      = -1;
  private String serverDir;
  private Map<String, Server> kafkaServers;
  private Map<String, Server> zookeeperServers;

  public KafkaCluster(String serverDir) throws Exception {
    this(serverDir, 1, 3);
  }


  public KafkaCluster(String serverDir, int numOfZkInstances, int numOfKafkaInstances) throws Exception {
    FileUtil.removeIfExist(serverDir, false);
    //deleteDirectory(new File(serverDir));
    this.serverDir = serverDir;
    this.numOfZkInstances = numOfZkInstances;
    this.numOfKafkaInstances = numOfKafkaInstances;
    zookeeperServers = new HashMap<String, Server>();
    kafkaServers = new HashMap<String, Server>();
  }

  public KafkaCluster setBaseZKPort(int port) {
    this.baseZKPort = port;
    return this;
  }

  public KafkaCluster setBaseKafkaPort(int port) {
    this.baseKafkaPort = port;
    return this;
  }
  
  public KafkaCluster setNumOfPartition(int number) {
    this.numOfPartitions = number;
    return this;
  }
  
  public KafkaCluster setReplication(int replication) {
    this.replication = replication;
    return this;
  }

  public void start() throws Exception {
    for (int i = 0; i < numOfZkInstances; i++) {
      String serverName = "zookeeper-" + (i + 1);
      ZookeeperServerLauncher zookeeper =
          new ZookeeperServerLauncher(serverDir + "/" + serverName, baseZKPort + i);
      zookeeper.start();
      zookeeperServers.put(serverName, zookeeper);
    }
    
    Thread.sleep(2000);
    
    for (int i = 0; i < numOfKafkaInstances; i++) {
      if(replication <= 0) {
        replication = 1;
        if(numOfKafkaInstances > 1) replication = 2;
      }
      
      int id = i + 1;
      String serverName = "kafka-" + id;
      KafkaServerLauncher kafka = new KafkaServerLauncher(id, serverDir + "/" + serverName, baseKafkaPort + i);
      kafka.setReplication(replication);
      kafka.setNumOfPartition(numOfPartitions);
      kafka.start();
      kafkaServers.put(serverName, kafka);
    }
  }

  public static boolean deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (null != files) {
        for (int i = 0; i < files.length; i++) {
          if (files[i].isDirectory()) {
            deleteDirectory(files[i]);
          } else {
            files[i].delete();
          }
        }
      }
    }
    return (directory.delete());
  }

  public Map<String, Server> getKafkaServerMap() {
    return kafkaServers;
  }

  public Server[] getKafkaServers() {
    Server[] server = new Server[kafkaServers.size()];
    kafkaServers.values().toArray(server);
    return server;
  }

  public Map<String, Server> getzookeeperServers() {
    return zookeeperServers;
  }

  public void shutdown() {
    for (Server server : kafkaServers.values()) {
      server.shutdown();
    }

    for (Server server : zookeeperServers.values()) {
      server.shutdown();
    }
  }

  public String getZKConnect() {
    String ipAddress = "127.0.0.1";
    StringBuilder b = new StringBuilder();
    for (Server server : zookeeperServers.values()) {
      if (b.length() > 0) b.append(",");
      b.append(ipAddress).append(":").append(server.getPort());
    }
    return b.toString();
  }

  public String getKafkaConnect() {
    String ipAddress = "127.0.0.1";
    StringBuilder b = new StringBuilder();
    for (Server server : kafkaServers.values()) {
      if (b.length() > 0)
        b.append(",");
      b.append(ipAddress).append(":").append(server.getPort());
    }
    return b.toString();
  }
  
  public List<String> getKafkaConnectList() {
    List<String> holder = new ArrayList<String>();
    for (Server server : kafkaServers.values()) {
      holder.add("127.0.0.1:" + server.getPort());
    }
    return holder;
  }
}
