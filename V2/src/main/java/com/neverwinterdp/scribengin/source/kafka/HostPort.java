package com.neverwinterdp.scribengin.source.kafka;

public class HostPort {
  private String host;
  private int port;

  public HostPort(String host, String port) {
    this.host = host;
    this.port = Integer.parseInt(port);
  }

  public HostPort(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(host);
    sb.append(":");
    sb.append(port);
    return sb.toString();
  }
}

