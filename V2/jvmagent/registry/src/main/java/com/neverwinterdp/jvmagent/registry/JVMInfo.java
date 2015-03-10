package com.neverwinterdp.jvmagent.registry;

import java.net.InetAddress;

public class JVMInfo {
  private String hostname;
  private String jmxPort;

  public JVMInfo() throws Exception {
    jmxPort = System.getProperty("com.sun.management.jmxremote.port");
    hostname = InetAddress.getLocalHost().getHostName();
  }
  
  public String getHostname() { return hostname; }
  public void setHostname(String hostname) { this.hostname = hostname; }
  
  public String getJmxPort() { return jmxPort; }
  public void setJmxPort(String jmxPort) { this.jmxPort = jmxPort; }
  
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("JVMInfo: \n");
    b.append("  hostname = " + hostname).append("\n");
    b.append("  jmxPort = " + jmxPort);
    return b.toString();
  }
}
