package com.neverwinterdp.vm.event;

import static com.neverwinterdp.vm.event.VMEvent.VM_HEARTBEAT;
import static com.neverwinterdp.vm.event.VMEvent.VM_MASTER_ELECTION;
import static com.neverwinterdp.vm.event.VMEvent.VM_STATUS;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.election.LeaderElectionNodeWatcher;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.WaitingEventListener;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.event.VMEvent.VMAttr;
import com.neverwinterdp.vm.service.VMService;

public class VMWaitingEventListener extends WaitingEventListener {
  
  public VMWaitingEventListener(Registry registry) throws RegistryException {
    super("Assert sequence of event for VM", registry);
    registryListener.watch(VMService.LEADER_PATH, new VMLeaderElectedNodeWatcher(registry), true);
  }

  public void waitVMServiceStatus(String desc, VMService.Status status) throws Exception {
    String path = VMService.MASTER_PATH + "/status";
    add(desc, path, true, VMService.Status.class, status);
  }
  
  
  public void waitVMStatus(String desc, String vmName, VMStatus vmStatus) throws Exception {
    String path = VMService.getVMStatusPath(vmName);
    VMStatusNodeWatcher vmStatusWatcher = new VMStatusNodeWatcher(registry) {
      synchronized public void onChange(NodeEvent event, VMDescriptor descriptor, VMStatus status) {
        boolean heartBeat = false;
        try {
          heartBeat = registry.exists(event.getPath() + "/heartbeat");
        } catch(RegistryException e) {
          throw new RuntimeException(e);
        }
        VMEvent vmEvent = new VMEvent(VM_STATUS, event);
        vmEvent.attr(VMAttr.vmdescriptor, descriptor);
        vmEvent.attr(VMAttr.vmstatus, status);
        vmEvent.attr(VMAttr.heartbeat, heartBeat);
        VMWaitingEventListener.this.process(vmEvent);
      }
    };
    registryListener.watch(path, vmStatusWatcher, true);
    add(new VMStatusEventListener(desc, vmName, vmStatus));
  }
  
  public void waitHeartbeat(String desc, String vmName, boolean connected) throws RegistryException {
    String path = VMService.getVMHeartbeatPath(vmName);
    VMHeartbeatNodeWatcher watcher = new VMHeartbeatNodeWatcher(registry) {
      @Override
      synchronized public void onConnected(NodeEvent event, VMDescriptor vmDescriptor) {
        VMEvent vmEvent = new VMEvent(VM_HEARTBEAT, event);
        vmEvent.attr(VMAttr.vmdescriptor, vmDescriptor);
        vmEvent.attr(VMAttr.heartbeat, true);
        VMWaitingEventListener.this.process(vmEvent);
      }

      @Override
      synchronized public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor) {
        VMEvent vmEvent = new VMEvent(VM_HEARTBEAT, event);
        vmEvent.attr(VMAttr.vmdescriptor, vmDescriptor);
        vmEvent.attr(VMAttr.heartbeat, false);
        VMWaitingEventListener.this.process(vmEvent);
      }
    };
    registryListener.watch(path, watcher, true);
    add(new VMHeartbeatEventListener(desc, vmName, connected));
  }
  
  public void waitVMMaster(String desc, String vmName) throws Exception {
    add(new VMMasterElectionEventListener(desc, vmName));
  }
  
  public class VMLeaderElectedNodeWatcher extends LeaderElectionNodeWatcher<VMDescriptor> {
    public VMLeaderElectedNodeWatcher(Registry registry) {
      super(registry, VMDescriptor.class);
    }

    @Override
    public void onElected(NodeEvent event, VMDescriptor learderVMDescriptor) {
      VMEvent vmEvent = new VMEvent(VM_MASTER_ELECTION, event);
      vmEvent.attr(VMAttr.master_leader, learderVMDescriptor);
      VMWaitingEventListener.this.process(vmEvent);
      setComplete();
    }
  }
  
  static public class VMStatusEventListener extends VMEventListener {
    String   expectVMName;
    VMStatus expectVMStatus; 
    
    public VMStatusEventListener(String description, String vmName, VMStatus vmStatus) {
      super(description);
      this.expectVMName = vmName;
      this.expectVMStatus = vmStatus;
    }

    @Override
    public boolean process(VMEvent event) {
      if(!VM_STATUS.equals(event.getName())) return false;
      VMDescriptor vmDescriptor = event.attr(VMAttr.vmdescriptor);
      VMStatus status = event.attr(VMAttr.vmstatus);
      if(!expectVMName.equals(vmDescriptor.getVmConfig().getName())) return false;
      if(!expectVMStatus.equals(status)) return false;
      return true;
    }
  }
  
  static public class VMHeartbeatEventListener extends VMEventListener {
    String   expectVMName;
    boolean  connected ; 
    
    public VMHeartbeatEventListener(String description, String vmName, boolean connected) {
      super(description);
      this.expectVMName = vmName;
      this.connected = connected ;
    }

    @Override
    public boolean process(VMEvent event) {
      if(!VM_HEARTBEAT.equals(event.getName())) return false;
      VMDescriptor vmDescriptor = event.attr(VMAttr.vmdescriptor);
      boolean connected = event.attr(VMAttr.heartbeat);
      if(!expectVMName.equals(vmDescriptor.getVmConfig().getName())) return false;
      if(this.connected != connected) return false;
      return true;
    }
  }
  
  static public class VMMasterElectionEventListener extends VMEventListener {
    String   expectVMName;
    
    public VMMasterElectionEventListener(String description, String vmName) {
      super(description);
      this.expectVMName = vmName;
    }

    @Override
    public boolean process(VMEvent event) {
      if(!VM_MASTER_ELECTION.equals(event.getName())) return false;
      VMDescriptor vmDescriptor = event.attr(VMAttr.master_leader);
      if(!expectVMName.equals(vmDescriptor.getVmConfig().getName())) return false;
      return true;
    }
  }
}