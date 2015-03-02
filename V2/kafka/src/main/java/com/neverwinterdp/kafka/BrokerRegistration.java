package com.neverwinterdp.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BrokerRegistration {
  private int    version;
  private long   timestamp;
  private String brokerId;
  private int    jmx_port = -1;
  private String host;
  private int    port;
  
  public int getVersion() { return version; }
  public void setVersion(int version) { this.version = version; }
  
  public long getTimestamp() { return timestamp; }
  public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
  
  public String getBrokerId() { return brokerId; }
  public void setBrokerId(String brokerId) { this.brokerId = brokerId; }
  
  @JsonProperty("jmx_port")
  public int getJmxPort() { return jmx_port; }
  public void setJmxPort(int jmx_port) { this.jmx_port = jmx_port; }
  
  public String getHost() { return host; }
  public void setHost(String host) { this.host = host; }
  
  public int getPort() { return port; }
  public void setPort(int port) {  this.port = port; }
}
