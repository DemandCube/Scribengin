package com.neverwinterdp.vm.junit;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.junit.RegistryAssert;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMListener;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.service.VMServiceListener;

public class VMAssert {
  private VMClient vmClient;
  private RegistryAssert registryAssert ;
  private VMListener vmListener ;
  private VMServiceListener vmServiceListener;
  
  public VMAssert(VMClient vmClient) throws RegistryException {
    this.vmClient = vmClient;
    this.registryAssert = new RegistryAssert();
    
    vmListener = new VMListener(vmClient.getRegistry());
    vmListener.add(new VMAssertStatusListener());
    vmListener.add(new VMAssertHeartBeatListener());
    
    //vmServiceListener = new VMServiceListener(vmClient.getRegistry());
  }
  
  public VMAssert(RegistryAssert registryAssert, VMClient vmClient) {
    this.vmClient = vmClient;
    this.registryAssert = registryAssert;
  }
  
  public void assertVMStatus(String desc, String vmName, VMStatus vmStatus, boolean heartBeat) throws Exception {
    registryAssert.add(new VMAssertStatus(desc, vmName, vmStatus, heartBeat));
    vmListener.watch(vmName);
  }
  
  public void waitForEvents(long timeout) throws Exception {
    registryAssert.waitForEvents(timeout);
  }
  
  public class VMAssertStatusListener implements VMListener.StatusListener {
    @Override
    synchronized public void onChange(NodeEvent event, VMDescriptor descriptor, VMStatus status) {
      boolean heartBeat = false;
      try {
        heartBeat = vmClient.getRegistry().exists(event.getPath() + "/heartbeat");
      } catch (RegistryException e) {
        throw new RuntimeException(e);
      }
      VMAssertEvent vmEvent = new VMAssertEvent(event, descriptor, status, heartBeat);
      registryAssert.process(vmEvent);
    }
  }
  
  public class VMAssertHeartBeatListener implements VMListener.HeartBeatListener {
    @Override
    synchronized public void onConnected(NodeEvent event, VMDescriptor vmDescriptor, VMStatus status) {
      VMAssertEvent vmEvent = new VMAssertEvent(event, vmDescriptor, status, true);
      registryAssert.process(vmEvent);
    }

    @Override
    synchronized public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor, VMStatus status) {
      VMAssertEvent vmEvent = new VMAssertEvent(event, vmDescriptor, status, false);
      registryAssert.process(vmEvent);
    }
  }
  
  public class VMServiceMasterListener implements VMServiceListener.LeaderListener {
    @Override
    public void onElected(NodeEvent event, VMDescriptor learderVMDescriptor) {
      VMAssertEvent vmEvent = new VMAssertEvent(event, learderVMDescriptor, null, false);
      registryAssert.process(vmEvent);
    }
  }
  
  static public class VMAssertStatus extends VMAssertUnit {
    String   expectVMName;
    VMStatus expectVMStatus; 
    boolean heartBeat = false;
    
    public VMAssertStatus(String description, String vmName, VMStatus vmStatus, boolean heartBeat) {
      super(description);
      this.expectVMName = vmName;
      this.expectVMStatus = vmStatus;
      this.heartBeat = heartBeat;
    }

    @Override
    public boolean assertEvent(VMAssertEvent event) {
      if(!expectVMName.equals(event.getVmDescriptor().getVmConfig().getName())) return false;
      if(!expectVMStatus.equals(event.getVmStatus())) return false;
      if(heartBeat != event.getHeartBeat()) return false;
      return true;
    }
  }
}