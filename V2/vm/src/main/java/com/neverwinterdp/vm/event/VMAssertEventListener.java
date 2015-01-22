package com.neverwinterdp.vm.event;

import static com.neverwinterdp.vm.event.VMEvent.VM_HEARTBEAT;
import static com.neverwinterdp.vm.event.VMEvent.VM_MASTER_ELECTION;
import static com.neverwinterdp.vm.event.VMEvent.VM_STATUS;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.election.LeaderElectionNodeWatcher;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.SequenceEventListener;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMHeartbeatNodeWatcher;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.VMStatusNodeWatcher;
import com.neverwinterdp.vm.event.VMEvent.VMAttr;
import com.neverwinterdp.vm.service.VMService;

public class VMAssertEventListener extends SequenceEventListener {
  
  public VMAssertEventListener(Registry registry) throws RegistryException {
    super("Assert sequence of event for VM", registry);
    registryListener.watch(VMService.LEADER_PATH, new VMLeaderElectionNodeWatcher(registry), true);
  }
  
  public void assertVMStatus(String desc, String vmName, VMStatus vmStatus) throws Exception {
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
        VMAssertEventListener.this.process(vmEvent);
      }
    };
    registryListener.watch(path, vmStatusWatcher, true);
    add(new VMAssertStatus(desc, vmName, vmStatus));
  }
  
  public void assertHeartbeat(String desc, String vmName, boolean connected) throws Exception {
    String path = VMService.getVMHeartbeatPath(vmName);
    VMHeartbeatNodeWatcher watcher = new VMHeartbeatNodeWatcher(registry) {
      @Override
      synchronized public void onConnected(NodeEvent event, VMDescriptor vmDescriptor) {
        VMEvent vmEvent = new VMEvent(VM_HEARTBEAT, event);
        vmEvent.attr(VMAttr.vmdescriptor, vmDescriptor);
        vmEvent.attr(VMAttr.heartbeat, true);
        VMAssertEventListener.this.process(vmEvent);
      }

      @Override
      synchronized public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor) {
        VMEvent vmEvent = new VMEvent(VM_HEARTBEAT, event);
        vmEvent.attr(VMAttr.vmdescriptor, vmDescriptor);
        vmEvent.attr(VMAttr.heartbeat, false);
        VMAssertEventListener.this.process(vmEvent);
      }
    };
    registryListener.watch(path, watcher, true);
    add(new VMAssertHeartbeat(desc, vmName, connected));
  }
  
  public void assertVMMaster(String desc, String vmName) throws Exception {
    add(new VMAssertMasterElection(desc, vmName));
  }
  
  public class VMLeaderElectionNodeWatcher extends LeaderElectionNodeWatcher<VMDescriptor> {
    public VMLeaderElectionNodeWatcher(Registry registry) {
      super(registry, VMDescriptor.class);
    }

    @Override
    public void onElected(NodeEvent event, VMDescriptor learderVMDescriptor) {
      VMEvent vmEvent = new VMEvent(VM_MASTER_ELECTION, event);
      vmEvent.attr(VMAttr.master_leader, learderVMDescriptor);
      VMAssertEventListener.this.process(vmEvent);
    }
  }
  
  static public class VMAssertStatus extends VMEventListener {
    String   expectVMName;
    VMStatus expectVMStatus; 
    
    public VMAssertStatus(String description, String vmName, VMStatus vmStatus) {
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
  
  static public class VMAssertHeartbeat extends VMEventListener {
    String   expectVMName;
    boolean  connected ; 
    
    public VMAssertHeartbeat(String description, String vmName, boolean connected) {
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
  
  static public class VMAssertMasterElection extends VMEventListener {
    String   expectVMName;
    
    public VMAssertMasterElection(String description, String vmName) {
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