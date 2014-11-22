package com.neverwinterdp.vm;

public interface VMService {
  public void start() throws Exception;
  public void stop() throws Exception ;
  
  public VM[] getAllocatedVMResources() ;
  public VM allocate(int cpucore, int memory) throws Exception ;
  //TODO: asynchronous allocation
  public void release(VM vmresource) throws Exception ;
}
