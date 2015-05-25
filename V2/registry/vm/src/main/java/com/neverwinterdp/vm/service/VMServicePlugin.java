package com.neverwinterdp.vm.service;

import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;


public interface VMServicePlugin {
  public void killVM(VMService vmService, VMDescriptor descriptor) throws Exception ;
  public void shutdownVM(VMService vmService, VMDescriptor descriptor) throws Exception ;
  public void allocateVM(VMService vmService, VMConfig vmConfig) throws Exception ;
  public void shutdown() ;
}
