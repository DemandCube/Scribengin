package com.neverwinterdp.vm;


public class VMServiceRegistryManagementListener implements VMServiceRegistryListener {
  
  @Override
  public void onStatusChange(VMService vmService, VMDescriptor vmDescriptor, VMStatus status) {
    //System.out.println("Status " + vmDescriptor.getStoredPath() + " - " + status);
  }

  @Override
  public void onConnectHearbeat(VMService vmService, VMDescriptor vmDescriptor) {
    //System.err.println("Heartbeat connect " + vmDescriptor.getStoredPath());
  }

  @Override
  public void onDisconnectHearbeat(VMService vmService, VMDescriptor vmDescriptor) {
    //System.err.println("Heartbeat disconnect " + vmDescriptor.getStoredPath());
    try {
      vmService.unregister(vmDescriptor);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
