package com.neverwinterdp.vm.event;

import static com.neverwinterdp.vm.event.VMEvent.VM_HEARTBEAT;
import static com.neverwinterdp.vm.event.VMEvent.VM_MASTER_ELECTION;
import static com.neverwinterdp.vm.event.VMEvent.VM_STATUS;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.election.LeaderElectionNodeWatcher;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventMatcher;
import com.neverwinterdp.registry.event.WaitingEventListener;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.event.VMEvent.VMAttr;
import com.neverwinterdp.vm.service.VMService;

public class VMWaitingEventListenerNew extends WaitingEventListener {
  private WaitingNodeEventListener waitingEventListeners ;
  
  public VMWaitingEventListenerNew(Registry registry) throws RegistryException {
    super("Assert sequence of event for VM", registry);
    waitingEventListeners = new WaitingNodeEventListener(registry);
    
    registryListener.watch(VMService.LEADER_PATH, new VMLeaderElectedNodeWatcher(registry), true);
  }

  public void waitVMServiceStatus(String desc, VMService.Status status) throws Exception {
    waitingEventListeners.add(VMService.MASTER_PATH + "/status", status);
  }
  
  public void waitVMStatus(String desc, String vmName, VMStatus vmStatus) throws Exception {
    String path = VMService.getVMStatusPath(vmName);
    waitingEventListeners.add(path, vmStatus);
  }
  
  public void waitHeartbeat(String desc, String vmName, boolean connected) throws Exception {
    String path = VMService.getVMHeartbeatPath(vmName);
    if(connected) {
      waitingEventListeners.add(path, NodeEvent.Type.CREATE);
    } else {
      waitingEventListeners.add(path, NodeEvent.Type.DELETE);
    }
  }
  
  public void waitVMMaster(String desc, String vmName) throws Exception {
    NodeEventMatcher matcher = new NodeEventMatcher() {
      @Override
      public boolean matches(Node node, NodeEvent event) throws Exception {
        return false;
      }
    };
    waitingEventListeners.add(VMService.LEADER_PATH, vmName);
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
      VMWaitingEventListenerNew.this.process(vmEvent);
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