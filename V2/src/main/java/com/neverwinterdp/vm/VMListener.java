package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.service.VMService;

public class VMListener {
  private List<HeartBeatListener> vmHeartBeatListeners = new ArrayList<>();
  private List<StatusListener>    vmStatusListeners    = new ArrayList<>();
  private Registry registry;
  
  public VMListener(Registry registry) {
    this.registry = registry;
  }
  
  public void add(StatusListener listener) {
    vmStatusListeners.add(listener);
  }
  
  public void add(HeartBeatListener listener) {
    vmHeartBeatListeners.add(listener);
  }
  
  public void watch(VMDescriptor descriptor) throws Exception {
    watch(descriptor.getVmConfig().getName());
  }
  
  public void watch(String vmName) throws Exception {
    final String path = VMService.ALLOCATED_PATH + "/" + vmName + "/status";
    if(registry.exists(path)) {
      registry.watchModify(path, new VMStatusWatcher());
      registry.watchChildren(VMService.ALLOCATED_PATH + "/" + vmName + "/status", new VMHeartbeatWatcher());
    } else {
      registry.watchExists(path, new VMStatusWatcher());
      //register children watch when the status node is available
      registry.watchExists(path, new NodeWatcher() {
        @Override
        public void process(NodeEvent event) {
          if(event.getType() == NodeEvent.Type.CREATE) {
            try {
              registry.watchChildren(path, new VMHeartbeatWatcher());
            } catch (RegistryException e) {
              e.printStackTrace();
            }
          }
        }
      });
    }
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
        if(event.getType() == NodeEvent.Type.MODIFY || event.getType() == NodeEvent.Type.CREATE) {
          VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
          VMStatus vmStatus = registry.getDataAs(path, VMStatus.class);
          for(StatusListener sel : vmStatusListeners) {
            sel.onChange(event, vmDescriptor, vmStatus);
          }
          registry.watchModify(path, this);
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
          VMStatus vmStatus = registry.getDataAs(path, VMStatus.class);
          if(registry.exists(path + "/heartbeat")) {
            for(HeartBeatListener sel : vmHeartBeatListeners) {
              sel.onConnected(event, vmDescriptor, vmStatus);
            }
            registry.watchChildren(path, this);
          } else {
            for(HeartBeatListener sel : vmHeartBeatListeners) {
              sel.onDisconnected(event, vmDescriptor, vmStatus);
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
  
  static public interface StatusListener {
    public void onChange(NodeEvent event, VMDescriptor descriptor, VMStatus status) ;
  }
  
  static public interface HeartBeatListener {
    public void onConnected(NodeEvent event, VMDescriptor vmDescriptor, VMStatus status) ;
    public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor, VMStatus status);
  }

}
