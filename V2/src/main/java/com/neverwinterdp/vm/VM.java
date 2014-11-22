package com.neverwinterdp.vm;

public interface VM {
  public VMDescriptor getDescriptor();
  
  public void startApp(String vmAppClass, String[] args) throws Exception;
  public void stopApp() throws Exception;
  
  public void exit() throws Exception;
}