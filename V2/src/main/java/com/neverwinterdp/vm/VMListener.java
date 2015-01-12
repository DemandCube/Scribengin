package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.vm.service.VMService;

public class VMListener {
  private List<VMHeartBeatListener> vmHeartBeatListeners = new ArrayList<>();
  private List<VMStatusListener>    vmStatusListeners    = new ArrayList<>();
  private Registry registry;
  
  public VMListener(Registry registry) {
    this.registry = registry;
  }
  
  public void addListener(VMStatusListener listener) {
    vmStatusListeners.add(listener);
  }
  
  public void addListener(VMHeartBeatListener listener) {
    vmHeartBeatListeners.add(listener);
  }
  
  public void watch(VMDescriptor descriptor) throws Exception {
    Node statusNode = registry.get(VMService.ALLOCATED_PATH + "/" + descriptor.getVmConfig().getName() + "/status");
    statusNode.watch(new VMStatusWatcher());
    statusNode.watchChildren(new VMHeartbeatWatcher());
  }
  
  public void close() { registry = null ; }
  
  public boolean isClosed() { return registry == null ; }
  
  public class VMStatusWatcher implements NodeWatcher {
    @Override
    public void process(NodeEvent event) {
      if(isClosed()) return;
      try {
        String path = event.getPath();
        String descriptorPath = path.substring(0, path.lastIndexOf('/')) ;
        if(event.getType() == NodeEvent.Type.MODIFY) {
          VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
          VMStatus vmStatus = registry.getDataAs(path, VMStatus.class);
          for(VMStatusListener sel : vmStatusListeners) {
            sel.onChange(vmDescriptor, vmStatus);
          }
          registry.watch(path, this);
        } else  if(event.getType() == NodeEvent.Type.DELETE) {
        } else {
          System.out.println("Unknown event: " + path + " - " + event.getType());
        }
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
  
  public class VMHeartbeatWatcher implements NodeWatcher {
    @Override
    public void process(NodeEvent event) {
      if(isClosed()) return;
      try {
        String path = event.getPath();
        String descriptorPath = path.substring(0, path.lastIndexOf('/')) ;
        if(event.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
          VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
          if(registry.exists(path + "/heartbeat")) {
            for(VMHeartBeatListener sel : vmHeartBeatListeners) {
              sel.onConnected(vmDescriptor);
            }
            registry.watchChildren(path, this);
          } else {
            for(VMHeartBeatListener sel : vmHeartBeatListeners) {
              sel.onDisconnected(vmDescriptor);
            }
            //should not register the watch since the vm node should be removed
          }
        } else {
          System.out.println("Unknown event: " + path + " - " + event.getType());
        }
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
