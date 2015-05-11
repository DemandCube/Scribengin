package com.neverwinterdp.vm.service;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.RegistryListener;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.event.VMHeartbeatNodeWatcher;

@Singleton
@JmxBean("role=vm-manager, type=VMService, name=VMService")
public class VMService {
  final static public String ALL_PATH       = "/vm/instances/all";
  final static public String ACTIVE_PATH    = "/vm/instances/active";
  final static public String HISTORY_PATH   = "/vm/instances/history";
  
  final static public String MASTER_PATH    = "/vm/master";
  final static public String LEADER_PATH    = MASTER_PATH + "/leader";
  final static public String EVENTS_PATH    = "/vm/events";
  final static public String SHUTDOWN_EVENT_PATH = EVENTS_PATH + "/shutdown";
  
  static public enum Status { INIT, RUNNING, TERMINATED }
  
  @Inject
  private Registry registry;
  
  @Inject
  private VMServicePlugin plugin ;
  
  private RegistryListener registryListener;
  
  
  @PostConstruct
  public void onInit() throws Exception {
    registry.createIfNotExist(MASTER_PATH + "/status") ;
    registry.createIfNotExist(LEADER_PATH) ;
    registry.createIfNotExist(EVENTS_PATH) ;
    
    registry.createIfNotExist(ALL_PATH) ;
    registry.createIfNotExist(ACTIVE_PATH) ;
    registry.createIfNotExist(HISTORY_PATH) ;
    registry.setData(MASTER_PATH + "/status", Status.INIT);
    registryListener = new RegistryListener(registry);
  }
  
  public void shutdown() {
    plugin.shutdown(); 
    registry = null ;
  }
  
  public boolean isClosed() { return registry == null ; }
  
  public Registry getRegistry() { return this.registry; }
  
  public void setStatus(Status status) throws Exception {
    registry.setData(MASTER_PATH + "/status", status);
  }
  
  public List<VMDescriptor> getAllVMDescriptors() throws RegistryException {
    return registry.getChildrenAs(ALL_PATH, VMDescriptor.class) ;
  }
  
  public List<VMDescriptor> getActiveVMDescriptors() throws RegistryException {
    return getActiveVMDescriptors(registry) ;
  }
  
  public List<VMDescriptor> getHistoryVMDescriptors() throws RegistryException {
    return getHistoryVMDescriptors(registry) ;
  }
  
  
  public void register(VMDescriptor descriptor) throws Exception {
    register(registry, descriptor);
    watch(descriptor);
  }
  
  public void watch(VMDescriptor vmDescriptor) throws Exception {
    VMHeartbeatNodeWatcher heartbeatWatcher = new VMHeartbeatNodeWatcher(registry) {
      @Override
      public void onDisconnected(NodeEvent event, VMDescriptor vmDescriptor) {
        try {
          unregister(vmDescriptor);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    String heartbeatPath = getVMHeartbeatPath(vmDescriptor.getVmConfig().getName());
    registryListener.watch(heartbeatPath, heartbeatWatcher, true);
  }
  
  public void unregister(VMDescriptor descriptor) throws Exception {
    if(isClosed()) return;
    if(!registry.exists(descriptor.getRegistryPath())) {
      //TODO: fix this check by removing the watcher
      return;
    }
    //Copy the vm descriptor to the history path. This is not efficient, 
    //but zookeeper does not provide the move method
    Transaction transaction = registry.getTransaction();
    transaction.create(HISTORY_PATH + "/" + descriptor.getVmConfig().getName(), new byte[0], NodeCreateMode.PERSISTENT);
    transaction.delete(ACTIVE_PATH + "/" + descriptor.getVmConfig().getName());
    transaction.commit();
  }
  
  public boolean isRunning(VMDescriptor descriptor) throws Exception {
    Node statusNode = registry.get(ALL_PATH + "/" + descriptor.getVmConfig().getName() + "/status");
    if(!statusNode.exists()) return false;
    VMStatus status = statusNode.getDataAs(VMStatus.class);
    if(statusNode.hasChild("heartbeat")) {
      if(status != VMStatus.TERMINATED) return true;
    } 
    return false;
  }
  
  public void kill(VMDescriptor descriptor) throws Exception {
    plugin.killVM(this, descriptor);
  }

  public VMDescriptor allocate(VMConfig vmConfig) throws RegistryException, Exception {
    VMDescriptor vmDescriptor = new VMDescriptor(vmConfig);
    register(vmDescriptor);
    plugin.allocateVM(this, vmConfig);
    return vmDescriptor;
  }
  
  static public String getVMStatusPath(String vmName) { return ALL_PATH + "/" + vmName + "/status" ; }
  
  static public String getVMHeartbeatPath(String vmName) { return ALL_PATH + "/" + vmName + "/status/heartbeat" ; }
  
  static public List<VMDescriptor> getActiveVMDescriptors(Registry registry) throws RegistryException {
    List<String> names = registry.getChildren(ACTIVE_PATH) ;
    List<String> paths = new ArrayList<String>() ;
    for(int i = 0; i < names.size(); i++) {
      paths.add(ALL_PATH + "/" + names.get(i)) ;
    }
    return registry.getDataAs(paths, VMDescriptor.class);
  }
  
  static public List<VMDescriptor> getHistoryVMDescriptors(Registry registry) throws RegistryException {
    List<String> names = registry.getChildren(HISTORY_PATH) ;
    List<String> paths = new ArrayList<String>() ;
    for(int i = 0; i < names.size(); i++) {
      paths.add(ALL_PATH + "/" + names.get(i)) ;
    }
    return registry.getDataAs(paths, VMDescriptor.class);
  }
  
  static public List<VMDescriptor> getAllVMDescriptors(Registry registry) throws RegistryException {
    return registry.getChildrenAs(ALL_PATH, VMDescriptor.class);
  }
  
  static public void register(Registry registry, VMDescriptor descriptor) throws Exception {
    registry.createIfNotExist(ALL_PATH) ;
    registry.createIfNotExist(ACTIVE_PATH) ;
    registry.createIfNotExist(HISTORY_PATH) ;
    registry.createIfNotExist(LEADER_PATH) ;
    
    String vmPath  = ALL_PATH + "/" + descriptor.getVmConfig().getName();
    descriptor.setRegistryPath(vmPath);
    
    Transaction transaction = registry.getTransaction() ;
    transaction.create(vmPath, new byte[0], NodeCreateMode.PERSISTENT);
    transaction.setData(vmPath, descriptor) ;
    transaction.create(vmPath + "/status", VMStatus.ALLOCATED, NodeCreateMode.PERSISTENT);
    transaction.create(vmPath + "/commands", new byte[0], NodeCreateMode.PERSISTENT) ;
    transaction.create(ACTIVE_PATH + "/" + descriptor.getVmConfig().getName(), new byte[0], NodeCreateMode.PERSISTENT) ;
    transaction.commit();
  }
}