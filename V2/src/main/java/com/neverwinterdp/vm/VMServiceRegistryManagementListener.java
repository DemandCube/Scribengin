package com.neverwinterdp.vm;

public class VMServiceRegistryManagementListener implements VMServiceRegistryListener {

  @Override
  public void onStatusChange(VMService vmService, VMDescriptor vmDescriptor, VMStatus status) {
  }

  @Override
  public void onConnectHearbeat(VMService vmService, VMDescriptor vmDescriptor) {
  }

  @Override
  public void onDisconnectHearbeat(VMService vmService, VMDescriptor vmDescriptor) {
    try {
      vmService.unregister(vmDescriptor);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
