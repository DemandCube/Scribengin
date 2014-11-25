package com.neverwinterdp.vm;

import com.neverwinterdp.vm.client.VMClient;

public interface VMService {
  public VMClient getVMClient();
  
  public VMDescriptor[] getAllocatedVMs() ;
  
  public VMDescriptor allocate(VMConfig vmConfig) throws Exception ;
  
  //TODO: asynchronous allocation
  public void release(VMDescriptor descriptor) throws Exception ;
  
  public void appStart(VMDescriptor descriptor, String vmAppClass, String[] args) throws Exception;
  public void appStop(VMDescriptor descriptor) throws Exception;
  
  public void start() throws Exception;
  
  public void shutdown() throws Exception ;
}
