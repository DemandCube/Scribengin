package com.neverwinterdp.tool.server;

public interface Server {

  public String getHost();

  public int getPort();

  public String getConnectString() ;
  
  public void start() throws Exception;

  public void shutdown();

}