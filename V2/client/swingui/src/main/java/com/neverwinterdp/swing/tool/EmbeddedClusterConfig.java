package com.neverwinterdp.swing.tool;

public class EmbeddedClusterConfig {
  private ZookeeperConfig zookeeperConfig = new ZookeeperConfig();
  private KafkaConfig     kafkaConfig = new KafkaConfig();
  
  public ZookeeperConfig getZookeeperConfig() { return zookeeperConfig; }
  public void setZookeeperConfig(ZookeeperConfig zookeeperConfig) {
    this.zookeeperConfig = zookeeperConfig;
  }

  public KafkaConfig getKafkaConfig() { return kafkaConfig; }
  public void setKafkaConfig(KafkaConfig kafkaConfig) {
    this.kafkaConfig = kafkaConfig;
  }

  static public class KafkaConfig {
    private int     numOfInstances = 3;
    private int     startPort = 9092;
    
    public int getNumOfInstances() { return numOfInstances; }
    public void setNumOfInstances(int numOfInstances) { this.numOfInstances = numOfInstances; }
    
    public int getStartPort() { return startPort; }
    public void setStartPort(int startPort) { this.startPort = startPort; }
  }

  static public class ZookeeperConfig {
    private int     numOfInstances = 1;
    private int     startPort = 2181;
    
    public int getNumOfInstances() { return numOfInstances; }
    public void setNumOfInstances(int numOfInstances) { this.numOfInstances = numOfInstances; }
    
    public int getStartPort() { return startPort; }
    public void setStartPort(int startPort) { this.startPort = startPort; }
  }
}
