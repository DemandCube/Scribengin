package com.neverwinterdp.scribengin.service;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PreDestroy;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mycila.jmx.annotation.JmxBean;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.PathFilter;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.Transaction;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.event.DataChangeNodeWatcher;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.RegistryListener;
import com.neverwinterdp.scribengin.ScribenginIdTrackerService;
import com.neverwinterdp.scribengin.activity.AddDataflowMasterActivityBuilder;
import com.neverwinterdp.scribengin.activity.ScribenginActivityService;
import com.neverwinterdp.scribengin.activity.ShutdownDataflowMasterActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;

@Singleton
@JmxBean("role=scribengin-master, type=ScribenginService, dataflowName=ScribenginService")
public class ScribenginService {
  final static public String  SCRIBENGIN_PATH        = "/scribengin";

  final static public String  EVENTS_PATH            = SCRIBENGIN_PATH + "/events";
  final static public String  SHUTDOWN_EVENT_PATH    = EVENTS_PATH + "/shutdown";
  
  final static public String  ACTIVITIES_PATH        = SCRIBENGIN_PATH + "/activities";

  final static public String  LEADER_PATH            = SCRIBENGIN_PATH + "/master/leader";

  final static public String  DATAFLOWS_PATH         = SCRIBENGIN_PATH + "/dataflow";
  final static public String  DATAFLOWS_ALL_PATH     = DATAFLOWS_PATH  + "/all";
  final static public String  DATAFLOWS_HISTORY_PATH = DATAFLOWS_PATH  + "/history";
  final static public String  DATAFLOWS_ACTIVE_PATH  = DATAFLOWS_PATH  + "/active";
  
  private Registry         registry;

  private RegistryListener registryListener;

  private Node             dataflowsAllNode;
  private Node             dataflowsActiveNode;
  private Node             dataflowsHistoryNode;
  private AtomicLong       historyIdTracker;
  
  @Inject
  private ScribenginActivityService activityService;
  
  @Inject
  private ScribenginIdTrackerService idTrackerService ;
  
  @Inject
  public void onInit(Registry registry) throws Exception {
    this.registry = registry;
    this.registryListener = new RegistryListener(registry);

    registry.createIfNotExist(EVENTS_PATH);
    dataflowsAllNode     = registry.createIfNotExist(DATAFLOWS_ALL_PATH);
    dataflowsActiveNode  = registry.createIfNotExist(DATAFLOWS_ACTIVE_PATH);
    dataflowsHistoryNode = registry.createIfNotExist(DATAFLOWS_HISTORY_PATH);

    historyIdTracker = new AtomicLong(dataflowsHistoryNode.getChildren().size());
  }
  
  @PreDestroy
  public void onDestroy() throws Exception {
    registryListener.close();
  }
  
  public boolean deploy(DataflowDescriptor descriptor) throws Exception {
    Node dataflowNode = dataflowsAllNode.createChild(descriptor.getId(), descriptor, NodeCreateMode.PERSISTENT);
    dataflowsActiveNode.createChild(descriptor.getId(), NodeCreateMode.PERSISTENT);
    dataflowNode.createDescendantIfNotExists("master/leader");
    String dataflowStatusPath = getDataflowStatusPath(descriptor.getId());
    registryListener.watch(dataflowStatusPath, new DataflowStatusListener(registry));
    
    Activity activity = new AddDataflowMasterActivityBuilder().build(dataflowNode.getPath()) ;
    activityService.queue(activity);
    activityService.queue(activity);
    return true;
  }
  
  //TODO: use transaction
  public void moveToHistory(DataflowDescriptor descriptor) throws Exception {
    Transaction transaction = registry.getTransaction();
    transaction.createChild(dataflowsHistoryNode, descriptor.getId(), NodeCreateMode.PERSISTENT);
    transaction.deleteChild(dataflowsActiveNode, descriptor.getId());
    transaction.commit();
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
          //DataflowDescriptor dataflowDescriptor = dataflowNode.getDataAs(DataflowDescriptor.class);
          //moveToHistory(dataflowDescriptor) ;
          System.err.println("ShutdownDataflowMasterActivityBuilder: detect event and call shutdown activity") ;
          Activity activity = new ShutdownDataflowMasterActivityBuilder().build(dataflowNode.getPath()) ;
          activityService.queue(activity);
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.err.println("Scribengin service catch dataflow finish " + event.getPath()) ;
        setComplete();
      }
    }
  };
  
  static public String getDataflowPath(String dataflowId) { 
    return DATAFLOWS_ALL_PATH + "/" + dataflowId; 
  }
  
  static public String getDataflowStatusPath(String dataflowId) { 
    return getDataflowPath(dataflowId) + "/status" ; 
  }
  
  static public String getDataflowLeaderPath(String dataflowId) { 
    return getDataflowPath(dataflowId) + "/master/leader" ; 
  }
}
