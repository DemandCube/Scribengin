package com.neverwinterdp.swing.es;

public class ESClusterConfiguration {
  private int    numOfInstances = 1;
  private int    basePort       = 9300;
  private String baseDir        = "build/elasticsearch";

  public int getNumOfInstances() { return numOfInstances; }
  public void setNumOfInstances(int numOfInstances) {
    this.numOfInstances = numOfInstances;
  }
  
  public int getBasePort() { return basePort; }
  public void setBasePort(int basePort) {
    this.basePort = basePort;
  }
  
  public String getBaseDir() { return baseDir; }
  public void setBaseDir(String baseDir) {
    this.baseDir = baseDir;
  }
}
