package com.neverwinterdp.vm.junit;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.junit.AssertEvent;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;

public class VMAssertEvent extends AssertEvent {
  private VMDescriptor vmDescriptor;
  private VMStatus     vmStatus;
  private boolean      heartBeat;
  
  public VMAssertEvent(NodeEvent event, VMDescriptor vmDescriptor, VMStatus vmStatus, boolean heartBeat) {
    super(event);
    this.vmDescriptor = vmDescriptor;
    this.vmStatus = vmStatus;
    this.heartBeat = heartBeat;
  }

  public VMDescriptor getVmDescriptor() { return vmDescriptor; }

  public VMStatus getVmStatus() { return vmStatus; }
  
  public boolean getHeartBeat() { return this.heartBeat; }
}