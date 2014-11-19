package com.neverwinterdp.scribengin.vmresource;

public interface VMResourceAllocator {
  public void start() throws Exception;
  public void stop() throws Exception ;
  
  public VMResource[] getAllocatedVMResources() ;
  public VMResource allocate(int cpucore, int memory) throws Exception ;
  //TODO: asynchronous allocation
  public void release(VMResource vmresource) throws Exception ;
}
