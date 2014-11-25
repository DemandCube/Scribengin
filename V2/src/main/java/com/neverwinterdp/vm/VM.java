package com.neverwinterdp.vm;

public interface VM {
  public VMDescriptor getDescriptor();
  
  public VMRegistry getVMRegistry();
  
  public void appStart(String app, String[] args) throws Exception;
  public void appStop() throws Exception;
  
  public void exit() throws Exception;
}