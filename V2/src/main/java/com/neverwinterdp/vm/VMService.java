package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.command.VMCommand;

@Singleton
public class VMService {
  final static public String ALLOCATED_PATH = "/vm/allocated";
  final static public String HISTORY_PATH   = "/vm/history";
  final static public String LEADER_PATH    = "/vm/leader";
  
  private Registry registry;
  private VMClient vmClient;
  private List<VMServiceRegistryListener> listeners = new ArrayList<VMServiceRegistryListener>() ;
  
  @Inject
  private VMServicePlugin plugin ;
  
  @Inject
  public void init(Registry registry) throws Exception {
    this.registry = registry;
    registry.createIfNotExist(LEADER_PATH) ;
    registry.createIfNotExist(ALLOCATED_PATH) ;
    registry.createIfNotExist(HISTORY_PATH) ;
    vmClient = new VMClient(registry) ;
    addListener(new VMServiceRegistryManagementListener());
  }
  
  public void close() { registry = null ; }
  
  public boolean isClosed() { return registry == null ; }
  
  public Registry getRegistry() { return this.registry; }
  
  public VMClient getVMClient() { return this.vmClient; }
  
  public void addListener(VMServiceRegistryListener listener) {
    listeners.add(listener);
  }
  
  public VMDescriptor[] getAllocatedVMDescriptors() throws RegistryException {
    List<String> names = registry.getChildren(ALLOCATED_PATH) ;
    VMDescriptor[] descriptor = new VMDescriptor[names.size()];
    for(int i = 0; i < names.size(); i++) {
      String name = names.get(i) ;
      descriptor[i] = registry.getDataAs(ALLOCATED_PATH + "/" + name, VMDescriptor.class) ;
    }
    return descriptor;
  }
  
  public void register(VMDescriptor descriptor) throws Exception {
    Node vmNode = registry.create(ALLOCATED_PATH + "/" + descriptor.getVmConfig().getName(), NodeCreateMode.PERSISTENT);
    descriptor.setStoredPath(vmNode.getPath());
    vmNode.setData(descriptor);
    Node statusNode  = vmNode.createChild("status", VMStatus.ALLOCATED, NodeCreateMode.PERSISTENT);
    Node commandNode = vmNode.createChild("commands", NodeCreateMode.PERSISTENT);
    watch(descriptor);
  }
  
  public void unregister(VMDescriptor descriptor) throws Exception {
    //TODO: fix this check by removing the watcher
    if(!registry.exists(descriptor.getStoredPath())) return;
    //Copy the vm descriptor to the history path. This is not efficient, 
    //but zookeeper does not provide the move method
    Node vmNode = 
        registry.create(HISTORY_PATH + "/" + descriptor.getVmConfig().getName() + "-", NodeCreateMode.PERSISTENT_SEQUENTIAL);
    vmNode.setData(descriptor);
    
    //Recursively delete the vm data in the allocated path
    registry.rdelete(descriptor.getStoredPath());
  }
  
  public void watch(VMDescriptor descriptor) throws Exception {
    Node statusNode = registry.get(ALLOCATED_PATH + "/" + descriptor.getVmConfig().getName() + "/status");
    statusNode.watch(new VMStatusWatcher());
    statusNode.watchChildren(new VMHeartbeatWatcher());
  }
  
  public boolean isRunning(VMDescriptor descriptor) throws Exception {
    Node statusNode = registry.get(ALLOCATED_PATH + "/" + descriptor.getVmConfig().getName() + "/status");
    if(!statusNode.exists()) return false;
    VMStatus status = statusNode.getData(VMStatus.class);
    if(status == VMStatus.ALLOCATED) {
      return true;
    } else {
      return statusNode.hasChild("heartbeat");
    }
  }
  
  public void kill(VMDescriptor descriptor) throws Exception {
    plugin.onKill(this, descriptor);
  }

  public VMDescriptor allocate(VMConfig vmConfig) throws RegistryException, Exception {
    VMDescriptor vmDescriptor = new VMDescriptor(vmConfig);
    register(vmDescriptor);
    plugin.allocate(this, vmConfig);
    return vmDescriptor;
  }

  public boolean vmExit(VMDescriptor descriptor) throws Exception {
    Command exitCmd = new VMCommand.Shutdown() ;
    CommandResult<?> exitCmdResult = vmClient.execute(descriptor, exitCmd);
    return exitCmdResult.getResultAs(Boolean.class);
  }
  
  static public void register(Registry registry, VMDescriptor descriptor) throws Exception {
    registry.createIfNotExist(ALLOCATED_PATH) ;
    registry.createIfNotExist(LEADER_PATH) ;
    Node vmNode = registry.create(ALLOCATED_PATH + "/" + descriptor.getVmConfig().getName(), NodeCreateMode.PERSISTENT);
    descriptor.setStoredPath(vmNode.getPath());
    vmNode.setData(descriptor);
    Node statusNode  = vmNode.createChild("status", VMStatus.ALLOCATED, NodeCreateMode.PERSISTENT);
    Node commandNode = vmNode.createChild("commands", NodeCreateMode.PERSISTENT);
  }
  
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
          for(VMServiceRegistryListener sel : listeners) {
            sel.onStatusChange(VMService.this, vmDescriptor, vmStatus);
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
            for(VMServiceRegistryListener sel : listeners) {
              sel.onConnectHearbeat(VMService.this, vmDescriptor);
            }
            registry.watchChildren(path, this);
          } else {
            for(VMServiceRegistryListener sel : listeners) {
              sel.onDisconnectHearbeat(VMService.this, vmDescriptor);
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