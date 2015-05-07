package com.neverwinterdp.swing.tool;

public class ClusterConfig {
  private ZookeeperConfig zookeeperConfig ;
  private KafkaConfig     kafkaConfig ;
  
  public ZookeeperConfig getZookeeperConfig() { return zookeeperConfig; }
  public void setZookeeperConfig(ZookeeperConfig zookeeperConfig) {
    this.zookeeperConfig = zookeeperConfig;
  }

  public KafkaConfig getKafkaConfig() { return kafkaConfig; }
  public void setKafkaConfig(KafkaConfig kafkaConfig) {
    this.kafkaConfig = kafkaConfig;
  }

  static public class KafkaConfig {
    private int     numOfInstances;
    private boolean launchEmbedded;
    private int     startPort ;
    
    public int getNumOfInstances() { return numOfInstances; }
    public void setNumOfInstances(int numOfInstances) { this.numOfInstances = numOfInstances; }
    
    public boolean isLaunchEmbedded() { return launchEmbedded; }
    public void setLaunchEmbedded(boolean launchEmbedded) { this.launchEmbedded = launchEmbedded; }
    
    public int getStartPort() { return startPort; }
    public void setStartPort(int startPort) { this.startPort = startPort; }
  }

  static public class ZookeeperConfig {
    private int     numOfInstances;
    private boolean launchEmbedded;
    private int     startPort ;
    private String  zkConnects ;
    
    public int getNumOfInstances() { return numOfInstances; }
    public void setNumOfInstances(int numOfInstances) { this.numOfInstances = numOfInstances; }
    
    public boolean isLaunchEmbedded() { return launchEmbedded; }
    public void setLaunchEmbedded(boolean launchEmbedded) { this.launchEmbedded = launchEmbedded; }
    
    public int getStartPort() { return startPort; }
    public void setStartPort(int startPort) { this.startPort = startPort; }
    
    public String getZkConnects() { return zkConnects; }
    public void setZkConnects(String zkConnects) { this.zkConnects = zkConnects; }
  }
}
