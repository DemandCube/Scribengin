package com.neverwinterdp.vm.junit;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.junit.RegistryAssert;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMListener;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.service.VMServiceListener;
import static com.neverwinterdp.vm.junit.VMAssertEvent.*;

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
    
    vmServiceListener = new VMServiceListener(vmClient.getRegistry());
    vmServiceListener.add(new VMServiceMasterListener());
  }
  
  public VMAssert(RegistryAssert registryAssert, VMClient vmClient) {
    this.vmClient = vmClient;
    this.registryAssert = registryAssert;
  }
  
  public void assertVMStatus(String desc, String vmName, VMStatus vmStatus) throws Exception {
    registryAssert.add(new VMAssertStatus(desc, vmName, vmStatus));
    vmListener.watch(vmName);
  }
  
  public void assertHeartbeat(String desc, String vmName, boolean connected) throws Exception {
    registryAssert.add(new VMAssertHeartbeat(desc, vmName, connected));
    vmListener.watch(vmName);
  }
  
  public void assertMasterElection(String desc, String vmName) throws Exception {
    registryAssert.add(new VMAssertMasterElection(desc, vmName));
  }
  
  public void waitForEvents(long timeout) throws Exception {
    registryAssert.waitForEvents(timeout);
  }
  
  public void reset() {
    registryAssert.reset();
  }
  
  public class VMAssertStatusListener implements VMListener.StatusListener {
    @Override
    synchronized public void onChange(NodeEvent event, VMDescriptor descriptor, VMStatus status) {
      boolean heartBeat = false;
      try {
        heartBeat = vmClient.getRegistry().exists(event.getPath() + "/heartbeat");
      } catch(RegistryException e) {
        throw new RuntimeException(e);
      }
      VMAssertEvent vmEvent = new VMAssertEvent(VM_STATUS, event);
      vmEvent.attr(Attr.vmdescriptor, descriptor);
      vmEvent.attr(Attr.vmstatus, status);
      vmEvent.attr(Attr.heartbeat, heartBeat);
      registryAssert.process(vmEvent);
    }
  }
  
  public class VMAssertHeartBeatListener implements VMListener.HeartBeatListener {
    @Override
    synchronized public void onConnected(NodeEvent event, VMDescriptor vmDescriptor) {
      VMAssertEvent vmEvent = new VMAssertEvent(VM_HEARTBEAT, event);
      vmEvent.attr(Attr.vmdescriptor, vmDescriptor);
      vmEvent.attr(Attr.heartbeat, true);
      registryAssert.process(vmEvent);
    }

    @Override
    synchronized public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor) {
      VMAssertEvent vmEvent = new VMAssertEvent(VM_HEARTBEAT, event);
      vmEvent.attr(Attr.vmdescriptor, vmDescriptor);
      vmEvent.attr(Attr.heartbeat, false);
      registryAssert.process(vmEvent);
    }
  }
  
  public class VMServiceMasterListener implements VMServiceListener.LeaderListener {
    @Override
    public void onElected(NodeEvent event, VMDescriptor learderVMDescriptor) {
      VMAssertEvent vmEvent = new VMAssertEvent(VM_MASTER_ELECTION, event);
      vmEvent.attr(Attr.master_leader, learderVMDescriptor);
      registryAssert.process(vmEvent);
    }
  }
  
  static public class VMAssertStatus extends VMAssertUnit {
    String   expectVMName;
    VMStatus expectVMStatus; 
    
    public VMAssertStatus(String description, String vmName, VMStatus vmStatus) {
      super(description);
      this.expectVMName = vmName;
      this.expectVMStatus = vmStatus;
    }

    @Override
    public boolean assertEvent(VMAssertEvent event) {
      if(!VM_STATUS.equals(event.getName())) return false;
      VMDescriptor vmDescriptor = event.attr(Attr.vmdescriptor);
      VMStatus status = event.attr(Attr.vmstatus);
      if(!expectVMName.equals(vmDescriptor.getVmConfig().getName())) return false;
      if(!expectVMStatus.equals(status)) return false;
      return true;
    }
  }
  
  static public class VMAssertHeartbeat extends VMAssertUnit {
    String   expectVMName;
    boolean  connected ; 
    
    public VMAssertHeartbeat(String description, String vmName, boolean connected) {
      super(description);
      this.expectVMName = vmName;
      this.connected = connected ;
    }

    @Override
    public boolean assertEvent(VMAssertEvent event) {
      if(!VM_HEARTBEAT.equals(event.getName())) return false;
      VMDescriptor vmDescriptor = event.attr(Attr.vmdescriptor);
      boolean connected = event.attr(Attr.heartbeat);
      if(!expectVMName.equals(vmDescriptor.getVmConfig().getName())) return false;
      if(this.connected != connected) return false;
      return true;
    }
  }
  
  static public class VMAssertMasterElection extends VMAssertUnit {
    String   expectVMName;
    
    public VMAssertMasterElection(String description, String vmName) {
      super(description);
      this.expectVMName = vmName;
    }

    @Override
    public boolean assertEvent(VMAssertEvent event) {
      if(!VM_MASTER_ELECTION.equals(event.getName())) return false;
      VMDescriptor vmDescriptor = event.attr(Attr.master_leader);
      if(!expectVMName.equals(vmDescriptor.getVmConfig().getName())) return false;
      return true;
    }
  }
}