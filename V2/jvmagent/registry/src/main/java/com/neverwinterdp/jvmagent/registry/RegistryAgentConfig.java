package com.neverwinterdp.jvmagent.registry;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class RegistryAgentConfig {
  private String zookeeperConnect;

  public RegistryAgentConfig() {} 
  
  public RegistryAgentConfig(Properties props) {
    zookeeperConnect = props.getProperty("zookeeper.connect");
  }
  
  public RegistryAgentConfig(String propFile) throws FileNotFoundException, IOException {
    this(load(propFile));
  }

  public String getZookeeperConnect() { return zookeeperConnect; }
  public void setZookeeperConnect(String value) { zookeeperConnect = value; }


  static public Properties load(String file) throws FileNotFoundException, IOException {
    Properties props = new Properties() ;
    props.load(new FileInputStream(file));
    return props ;
  }
}
