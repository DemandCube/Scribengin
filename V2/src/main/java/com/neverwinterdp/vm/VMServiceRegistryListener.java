package com.neverwinterdp.vm;

public interface VMServiceRegistryListener {
  public void onStatusChange(VMService vmService, VMDescriptor vmDescriptor, VMStatus status) ;
  public void onConnectHearbeat(VMService vmService, VMDescriptor vmDescriptor) ;
  public void onDisconnectHearbeat(VMService vmService, VMDescriptor vmDescriptor) ;
}
