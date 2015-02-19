package com.neverwinterdp.vm.event;

import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.vm.VMDescriptor;

public class VMHeartbeatNodeWatcher extends NodeWatcher {
  private Registry registry;
  
  public VMHeartbeatNodeWatcher(Registry registry) {
    this.registry = registry;
  }
  
  @Override
  public void onEvent(NodeEvent event) {
    try {
      String path = event.getPath();
      String descriptorPath = path.replace("/status/heartbeat", "") ;
      if(event.getType() == NodeEvent.Type.CREATE) {
        VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
        onConnected(event, vmDescriptor);
      } else if(event.getType() == NodeEvent.Type.DELETE) {
        VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
        onDisconnected(event, vmDescriptor);
        //setComplete();
      }
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  public void onConnected(NodeEvent event, VMDescriptor vmDescriptor) {
  }
  
  public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor) {
  }
}