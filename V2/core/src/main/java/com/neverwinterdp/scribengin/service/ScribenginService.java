package com.neverwinterdp.scribengin.service;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.DataChangeNodeWatcher;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.RegistryListener;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.service.VMDataflowServiceApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.service.VMServiceCommand;

@Singleton
public class ScribenginService {
  final static public String SCRIBENGIN_PATH         = "/scribengin";
  final static public String EVENTS_PATH             = SCRIBENGIN_PATH + "/events";
  final static public String LEADER_PATH             = SCRIBENGIN_PATH + "/master/leader";
  final static public String DATAFLOWS_HISTORY_PATH  = SCRIBENGIN_PATH + "/dataflows/history";
  final static public String DATAFLOWS_RUNNING_PATH  = SCRIBENGIN_PATH + "/dataflows/running";
  
  @Inject
  private VMConfig vmConfig; 
  private Registry registry;
  private VMClient vmClient ;
  private RegistryListener registryListener ;
  
  private Node dataflowsRunningNode ;
  private Node dataflowsHistoryNode ;
  private AtomicLong historyIdTracker ;
  
  @Inject
  public void onInit(Registry registry, VMClient vmClient) throws Exception {
    this.registry = registry;
    this.registryListener = new RegistryListener(registry);

    registry.createIfNotExist(EVENTS_PATH);
    registry.createIfNotExist(DATAFLOWS_RUNNING_PATH);
    dataflowsRunningNode = registry.get(DATAFLOWS_RUNNING_PATH) ;

    registry.createIfNotExist(DATAFLOWS_HISTORY_PATH);
    dataflowsHistoryNode = registry.get(DATAFLOWS_HISTORY_PATH) ;
    historyIdTracker = new AtomicLong(dataflowsHistoryNode.getChildren().size());
    this.vmClient = vmClient;
  }
  
  public void onDestroy() throws Exception {
  }
  
  public boolean deploy(DataflowDescriptor descriptor) throws Exception {
    Node dataflowNode = dataflowsRunningNode.createChild(descriptor.getName(), descriptor, NodeCreateMode.PERSISTENT);
    dataflowNode.createDescendantIfNotExists("master/leader");
    String dataflowStatusPath = getDataflowStatusPath(descriptor.getName());
    registryListener.watch(dataflowStatusPath, new DataflowStatusListener(registry));
    DataflowDeployer deployer = new DataflowDeployer(descriptor);
    deployer.start();
    return true;
  }
  
  private VMDescriptor createDataflowMaster(DataflowDescriptor descriptor, int id) throws Exception {
    String dataflowAppHome = descriptor.getDataflowAppHome();
    Node dataflowNode = registry.get(DATAFLOWS_RUNNING_PATH + "/" + descriptor.getName()) ;
    VMConfig dfVMConfig = new VMConfig() ;
    if(dataflowAppHome != null) {
      dfVMConfig.setAppHome(dataflowAppHome);
      dfVMConfig.addVMResource("dataflow.libs", dataflowAppHome + "/libs");
    }
    dfVMConfig.setEnvironment(vmConfig.getEnvironment());
    dfVMConfig.setName(descriptor.getName() + "-master-" + id);
    dfVMConfig.setRoles(Arrays.asList("dataflow-master"));
    dfVMConfig.setRegistryConfig(registry.getRegistryConfig());
    dfVMConfig.setVmApplication(VMDataflowServiceApp.class.getName());
    dfVMConfig.addProperty("dataflow.registry.path", dataflowNode.getPath());
    dfVMConfig.setHadoopProperties(vmConfig.getHadoopProperties());
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    CommandResult<VMDescriptor> result = 
        (CommandResult<VMDescriptor>)vmClient.execute(masterVMDescriptor, new VMServiceCommand.Allocate(dfVMConfig));
    return result.getResult();
  }
  
  private void moveToHistory(DataflowDescriptor descriptor) throws Exception {
    String fromPath = dataflowsRunningNode.getPath() + "/" + descriptor.getName();
    String toPath   = dataflowsHistoryNode.getPath() + "/" + descriptor.getName() + "-" + historyIdTracker.getAndIncrement();
    registry.rcopy(fromPath, toPath);
    registry.rdelete(fromPath);
  }
  
  class DataflowStatusListener extends DataChangeNodeWatcher<DataflowLifecycleStatus> {
    public DataflowStatusListener(Registry registry) {
      super(registry, DataflowLifecycleStatus.class);
    }
    
    @Override
    public void onChange(NodeEvent event, DataflowLifecycleStatus status) {
      if(DataflowLifecycleStatus.FINISH == status) {
        try {
          Node statusNode = registry.get(event.getPath());
          Node dataflowNode = statusNode.getParentNode() ;
          DataflowDescriptor dataflowDescriptor = dataflowNode.getDataAs(DataflowDescriptor.class);
          moveToHistory(dataflowDescriptor) ;
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.err.println("Scribengin service catch dataflow finish " + event.getPath()) ;
        setComplete();
      }
    }
  };
  
  public class DataflowDeployer extends Thread {
    private DataflowDescriptor descriptor;
    
    public DataflowDeployer(DataflowDescriptor descriptor) {
      this.descriptor      = descriptor;
    }
    
    public void run() {
      try {
        VMDescriptor dataflowMaster1 = createDataflowMaster(descriptor, 1);
        //VMDescriptor master2 = createDataflowMaster(descriptor, 2);
      } catch(Exception ex) {
        ex.printStackTrace();
      }
    }
  }
  
  
  
  static public String getDataflowPath(String dataflowName) { 
    return DATAFLOWS_RUNNING_PATH + "/" + dataflowName; 
  }
  
  static public String getDataflowStatusPath(String dataflowName) { 
    return getDataflowPath(dataflowName) + "/status" ; 
  }
  
  static public String getDataflowLeaderPath(String dataflowName) { 
    return getDataflowPath(dataflowName) + "/master/leader" ; 
  }
}
