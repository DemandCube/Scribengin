package com.neverwinterdp.vm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
  final static public String LEADER_PATH    = "/vm/leader";
  
  private Registry registry;
  private VMClient vmClient;
  private List<VMServiceRegistryListener> listeners = new ArrayList<VMServiceRegistryListener>() ;
  
  @Inject
  private VMServicePlugin plugin ;
  
  @Inject
  public void init(Registry registry) throws Exception {
    this.registry = registry;
    registry.createIfNotExist(ALLOCATED_PATH) ;
    registry.createIfNotExist(LEADER_PATH) ;
    vmClient = new VMClient(registry) ;
    addListener(new VMServiceRegistryManagementListener());
  }
  
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
    VMStatusNodeWatcher statusWatcher = new VMStatusNodeWatcher() ;
    statusNode.watch(statusWatcher);
    statusNode.watchChildren(statusWatcher);
    Node commandNode = vmNode.createChild("commands", NodeCreateMode.PERSISTENT);
  }
  
  public void unregister(VMDescriptor descriptor) throws Exception {
    registry.rdelete(descriptor.getStoredPath());
  }
  
  public void release(VMDescriptor descriptor) throws Exception {
    plugin.onRelease(this, descriptor);
  }

  public void allocate(VM vm) throws Exception {
    VMDescriptor vmDescriptor = vm.getDescriptor();
    register(vmDescriptor);
    plugin.onRegisterVM(this, vm);
  }
  
  public VMDescriptor allocate(VMConfig vmConfig) throws RegistryException, Exception {
    VMDescriptor vmDescriptor = new VMDescriptor(vmConfig);
    register(vmDescriptor);
    vmDescriptor = plugin.allocate(this, vmDescriptor);
    return vmDescriptor;
  }

  public VMStatus appStart(VMDescriptor descriptor, String vmAppClass, Map<String, String> props) throws Exception {
    Command startCmd = new VMCommand.AppStart(vmAppClass, props) ;
    CommandResult<?> startCmdResult = vmClient.execute(descriptor, startCmd);
    return startCmdResult.getResultAs(VMStatus.class);
  }
  
  public VMStatus appStop(VMDescriptor descriptor) throws Exception {
    Command stopCmd = new VMCommand.AppStop() ;
    CommandResult<?> stopCmdResult = vmClient.execute(descriptor, stopCmd);
    return stopCmdResult.getResultAs(VMStatus.class);
  }
  
  public boolean vmExit(VMDescriptor descriptor) throws Exception {
    Command exitCmd = new VMCommand.Exit() ;
    CommandResult<?> exitCmdResult = vmClient.execute(descriptor, exitCmd);
    return exitCmdResult.getResultAs(Boolean.class);
  }
  
  public class VMStatusNodeWatcher implements NodeWatcher {
    @Override
    public void process(NodeEvent event) {
      try {
        String path = event.getPath();
        String descriptorPath = path.substring(0, path.lastIndexOf('/')) ;
        if(event.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
          //TODO: need to find a way to unwatch when a vm is released
          if(!registry.exists(descriptorPath)) return;
          
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
        } else if(event.getType() == NodeEvent.Type.MODIFY) {
          VMDescriptor vmDescriptor = registry.getDataAs(descriptorPath, VMDescriptor.class);
          VMStatus vmStatus = registry.getDataAs(path, VMStatus.class);
          for(VMServiceRegistryListener sel : listeners) {
            sel.onStatusChange(VMService.this, vmDescriptor, vmStatus);
          }
          registry.watch(path, this);
        } else {
          System.out.println("Unknown event: " + path + " - " + event.getType());
        }
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}