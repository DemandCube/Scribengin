package com.neverwinterdp.scribengin.vmresource;

public interface VMResource {
  public VMResourceDescriptor getDescriptor();
  
  public void startApp(String vmAppClass, String[] args) throws Exception;
  public void stopApp() throws Exception;
  
  public void exit() throws Exception;
}