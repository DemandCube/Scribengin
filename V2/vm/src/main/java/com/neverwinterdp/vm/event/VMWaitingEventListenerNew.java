package com.neverwinterdp.vm.event;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventMatcher;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.service.VMService;

public class VMWaitingEventListenerNew {
  private WaitingNodeEventListener waitingEventListeners ;
  
  public VMWaitingEventListenerNew(Registry registry) throws RegistryException {
    waitingEventListeners = new WaitingNodeEventListener(registry);
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
  
  public void waitVMMaster(String desc, final String vmName) throws Exception {
    NodeEventMatcher matcher = new NodeEventMatcher() {
      @Override
      public boolean matches(Node node, NodeEvent event) throws Exception {
        if(event.getType() == NodeEvent.Type.MODIFY) {
          Node refNode = node.getRegistry().getRef(node.getPath());
          return refNode.getName().equals(vmName);
        }
        return false;
      }
    };
    waitingEventListeners.add(VMService.LEADER_PATH, matcher);
  }
  
  public void waitForEvents(long timeout) throws Exception {
    waitingEventListeners.waitForEvents(timeout);
  }
}