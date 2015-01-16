package com.neverwinterdp.vm.junit;

import static com.neverwinterdp.vm.junit.VMAssertEvent.VM_HEARTBEAT;
import static com.neverwinterdp.vm.junit.VMAssertEvent.VM_MASTER_ELECTION;
import static com.neverwinterdp.vm.junit.VMAssertEvent.VM_STATUS;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.election.RegistryLeaderElectionListener;
import com.neverwinterdp.registry.junit.RegistryAssert;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMRegistryListener;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.junit.VMAssertEvent.VMAttr;
import com.neverwinterdp.vm.service.VMServiceRegistryListener;

public class VMAssert extends RegistryAssert {
  private Registry registry;
  private VMRegistryListener vmListener ;
  private VMServiceRegistryListener vmServiceListener;
  
  public VMAssert(Registry registry) throws RegistryException {
    this.registry = registry;
    
    vmListener = new VMRegistryListener(registry);
    vmListener.add(new VMAssertStatusListener());
    vmListener.add(new VMAssertHeartBeatListener());
    
    vmServiceListener = new VMServiceRegistryListener(registry);
    vmServiceListener.add(new VMServiceMasterListener());
  }
  
  public void assertVMStatus(String desc, String vmName, VMStatus vmStatus) throws Exception {
    add(new VMAssertStatus(desc, vmName, vmStatus));
    vmListener.watch(vmName);
  }
  
  public void assertHeartbeat(String desc, String vmName, boolean connected) throws Exception {
    add(new VMAssertHeartbeat(desc, vmName, connected));
    vmListener.watch(vmName);
  }
  
  public void assertMasterElection(String desc, String vmName) throws Exception {
    add(new VMAssertMasterElection(desc, vmName));
  }
  
  public class VMAssertStatusListener implements VMRegistryListener.StatusListener {
    @Override
    synchronized public void onChange(NodeEvent event, VMDescriptor descriptor, VMStatus status) {
      boolean heartBeat = false;
      try {
        heartBeat = registry.exists(event.getPath() + "/heartbeat");
      } catch(RegistryException e) {
        throw new RuntimeException(e);
      }
      VMAssertEvent vmEvent = new VMAssertEvent(VM_STATUS, event);
      vmEvent.attr(VMAttr.vmdescriptor, descriptor);
      vmEvent.attr(VMAttr.vmstatus, status);
      vmEvent.attr(VMAttr.heartbeat, heartBeat);
      process(vmEvent);
    }
  }
  
  public class VMAssertHeartBeatListener implements VMRegistryListener.HeartBeatListener {
    @Override
    synchronized public void onConnected(NodeEvent event, VMDescriptor vmDescriptor) {
      VMAssertEvent vmEvent = new VMAssertEvent(VM_HEARTBEAT, event);
      vmEvent.attr(VMAttr.vmdescriptor, vmDescriptor);
      vmEvent.attr(VMAttr.heartbeat, true);
      process(vmEvent);
    }

    @Override
    synchronized public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor) {
      VMAssertEvent vmEvent = new VMAssertEvent(VM_HEARTBEAT, event);
      vmEvent.attr(VMAttr.vmdescriptor, vmDescriptor);
      vmEvent.attr(VMAttr.heartbeat, false);
      process(vmEvent);
    }
  }
  
  public class VMServiceMasterListener implements RegistryLeaderElectionListener.LeaderListener<VMDescriptor> {
    @Override
    public void onElected(NodeEvent event, VMDescriptor learderVMDescriptor) {
      VMAssertEvent vmEvent = new VMAssertEvent(VM_MASTER_ELECTION, event);
      vmEvent.attr(VMAttr.master_leader, learderVMDescriptor);
      process(vmEvent);
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
      VMDescriptor vmDescriptor = event.attr(VMAttr.vmdescriptor);
      VMStatus status = event.attr(VMAttr.vmstatus);
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
      VMDescriptor vmDescriptor = event.attr(VMAttr.vmdescriptor);
      boolean connected = event.attr(VMAttr.heartbeat);
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
      VMDescriptor vmDescriptor = event.attr(VMAttr.master_leader);
      if(!expectVMName.equals(vmDescriptor.getVmConfig().getName())) return false;
      return true;
    }
  }
}