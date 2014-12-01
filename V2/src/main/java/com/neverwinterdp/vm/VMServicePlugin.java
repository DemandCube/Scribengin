package com.neverwinterdp.vm;


public interface VMServicePlugin {
  public void onRegisterVM(VMService vmService, VM vm) throws Exception;
  public void onUnregister(VMService vmService, VM vm) throws Exception;
  public void onRelease(VMService vmService, VMDescriptor descriptor) throws Exception ;
  public VMDescriptor allocate(VMService vmService, VMDescriptor vmDescriptor) throws Exception ;
}
