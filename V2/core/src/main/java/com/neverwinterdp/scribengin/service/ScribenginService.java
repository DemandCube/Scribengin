package com.neverwinterdp.scribengin.service;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PreDestroy;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.PathFilter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.event.DataChangeNodeWatcher;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.RegistryListener;
import com.neverwinterdp.scribengin.activity.AddDataflowMasterActivityBuilder;
import com.neverwinterdp.scribengin.activity.ScribenginActivityService;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowActivityService;
import com.neverwinterdp.scribengin.dataflow.service.VMDataflowServiceApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.service.VMServiceCommand;

@Singleton
@JmxBean("role=scribengin-master, type=ScribenginService, dataflowName=ScribenginService")
public class ScribenginService {
  final static public String SCRIBENGIN_PATH         = "/scribengin";
  
  final static public String EVENTS_PATH             = SCRIBENGIN_PATH + "/events";
  final static public String ACTIVITIES_PATH          = SCRIBENGIN_PATH + "/activities";
  final static public String SHUTDOWN_EVENT_PATH     = EVENTS_PATH + "/shutdown";

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
  private ScribenginActivityService activityService;
  
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
  
  @PreDestroy
  public void onDestroy() throws Exception {
    registryListener.close();
  }
  
  public boolean deploy(DataflowDescriptor descriptor) throws Exception {
    Node dataflowNode = dataflowsRunningNode.createChild(descriptor.getName(), descriptor, NodeCreateMode.PERSISTENT);
    dataflowNode.createDescendantIfNotExists("master/leader");
    Activity activity = new AddDataflowMasterActivityBuilder().build(dataflowNode.getPath()) ;
    String dataflowStatusPath = getDataflowStatusPath(descriptor.getName());
    registryListener.watch(dataflowStatusPath, new DataflowStatusListener(registry));
    activityService.queue(activity);
    return true;
  }
  
  //TODO: use transaction
  private void moveToHistory(DataflowDescriptor descriptor) throws Exception {
    String fromPath = dataflowsRunningNode.getPath() + "/" + descriptor.getName();
    String toPath   = dataflowsHistoryNode.getPath() + "/" + descriptor.getName() + "-" + historyIdTracker.getAndIncrement();
    PathFilter ignoreLeader = new PathFilter.IgnorePathFilter(".*/leader/leader-.*") ;
    registry.rcopy(fromPath, toPath, ignoreLeader);
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
