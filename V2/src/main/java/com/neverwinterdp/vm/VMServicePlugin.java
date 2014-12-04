package com.neverwinterdp.vm;


public interface VMServicePlugin {
  public void onKill(VMService vmService, VMDescriptor descriptor) throws Exception ;
  public void allocate(VMService vmService, VMConfig vmConfig) throws Exception ;
}
