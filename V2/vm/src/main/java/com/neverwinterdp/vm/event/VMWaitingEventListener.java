package com.neverwinterdp.vm.event;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventMatcher;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.service.VMService;

public class VMWaitingEventListener {
  protected WaitingNodeEventListener waitingEventListeners ;
  
  public VMWaitingEventListener(Registry registry) throws RegistryException {
    waitingEventListeners = new WaitingNodeEventListener(registry);
  }

  public WaitingNodeEventListener getWaitingNodeEventListener() { return waitingEventListeners; }
  
  public void waitVMServiceStatus(String desc, VMService.Status status) throws Exception {
    waitingEventListeners.add(VMService.MASTER_PATH + "/status", status, desc);
  }
  
  public void waitVMStatus(String desc, String vmName, VMStatus vmStatus) throws Exception {
    String path = VMService.getVMStatusPath(vmName);
    waitingEventListeners.add(path, vmStatus, desc);
  }
  
  public void waitHeartbeat(String desc, String vmName, boolean connected) throws Exception {
    String path = VMService.getVMHeartbeatPath(vmName);
    if(connected) {
      waitingEventListeners.add(path, NodeEvent.Type.CREATE, desc);
    } else {
      waitingEventListeners.add(path, NodeEvent.Type.DELETE, desc);
    }
  }
  
  public void waitVMMaster(String desc, final String vmName) throws Exception {
    waitingEventListeners.add(VMService.LEADER_PATH, new VMLeaderNodeEventMatcher(vmName), desc);
  }
  
  public void waitForEvents(long timeout) throws Exception {
    waitingEventListeners.waitForEvents(timeout);
  }
  
  public TabularFormater getTabularFormaterEventLogInfo()  {
    return waitingEventListeners.getTabularFormaterEventLogInfo();
  }
  
  static public class VMLeaderNodeEventMatcher implements NodeEventMatcher {
    private String vmName  ;
    
    public VMLeaderNodeEventMatcher(String vmName) {
      this.vmName = vmName ;
    }
    
    @Override
    public boolean matches(Node node, NodeEvent event) throws Exception {
      if(event.getType() == NodeEvent.Type.MODIFY) {
        Node refNode = node.getRegistry().getRef(node.getPath());
        return refNode.getName().equals(vmName);
      }
      return false;
    }
  }
}