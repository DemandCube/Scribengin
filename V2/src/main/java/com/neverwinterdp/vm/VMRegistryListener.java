package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.vm.service.VMService;

public class VMRegistryListener {
  private List<HeartBeatListener> vmHeartBeatListeners = new ArrayList<>();
  private List<StatusListener>    vmStatusListeners    = new ArrayList<>();
  private Set<String> watchedVMs = new HashSet<>();
  
  private Registry registry;
  
  public VMRegistryListener(Registry registry) {
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
  
  public void watch(final String vmName) throws Exception {
    if(watchedVMs.contains(vmName)) return;
    final String path = VMService.ALLOCATED_PATH + "/" + vmName + "/status";
    if(registry.exists(path)) {
      registry.watchModify(path, new VMStatusWatcher());
    } else {
      registry.watchExists(path, new VMStatusWatcher());
    }
    registry.watchExists(VMService.ALLOCATED_PATH + "/" + vmName + "/status/heartbeat", new VMHeartbeatWatcher());
    watchedVMs.add(vmName);
  }
  
  public void unwatch(String vmName) throws Exception {
    if(!watchedVMs.contains(vmName)) return;
    final String path = VMService.ALLOCATED_PATH + "/" + vmName + "/status";
    if(registry.exists(path)) {
      registry.watchExists(path, null);
    } else {
      registry.watchModify(path, null);
      registry.watchChildren(path, null);
    }
    watchedVMs.remove(vmName) ;
  }
  
  public void close() { registry = null ; }
  
  public boolean isClosed() { return registry == null ; }
  
  public class VMStatusWatcher implements NodeWatcher {
    @Override
    public void process(NodeEvent event) {
      if(isClosed()) return;
      //System.out.println("got status event " + event.getPath());
      try {
        String path = event.getPath();
        String descriptorPath = path.substring(0, path.lastIndexOf('/')) ;
        if(event.getType() == NodeEvent.Type.MODIFY || event.getType() == NodeEvent.Type.CREATE) {
          registry.watchModify(path, this);
          VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
          VMStatus vmStatus = registry.getDataAs(path, VMStatus.class);
          for(StatusListener sel : vmStatusListeners) {
            sel.onChange(event, vmDescriptor, vmStatus);
          }
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
      //System.out.println("got heartbeat event " + event.getPath());
      if(isClosed()) return;
      try {
        String path = event.getPath();
        String descriptorPath = path.replace("/status/heartbeat", "") ;
        if(event.getType() == NodeEvent.Type.CREATE) {
          registry.watchExists(path, this);
          VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
          for(HeartBeatListener sel : vmHeartBeatListeners) {
            sel.onConnected(event, vmDescriptor);
          }
        } else if(event.getType() == NodeEvent.Type.DELETE) {
          VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
          for(HeartBeatListener sel : vmHeartBeatListeners) {
            sel.onDisconnected(event, vmDescriptor);
          }
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
    public void onConnected(NodeEvent event, VMDescriptor vmDescriptor) ;
    public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor);
  }

}
