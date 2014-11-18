package com.neverwinterdp.scribengin.vmresource;

public interface VMResource {
  public long getId();
  public int  getMemory() ;
  public int  getCpuCores() ;
  public String getHostname();

  public void startApp(String vmAppClass, String[] args) throws Exception;
  public void stopApp() throws Exception;
  
  public void exit() throws Exception;
}
