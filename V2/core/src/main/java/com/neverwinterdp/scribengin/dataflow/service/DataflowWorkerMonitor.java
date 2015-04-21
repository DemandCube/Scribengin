package com.neverwinterdp.scribengin.dataflow.service;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.activity.AddWorkerActivityBuilder;
import com.neverwinterdp.scribengin.dataflow.activity.DataflowActivityService;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowWorkerMonitor {
  private DataflowRegistry dataflowRegistry ;
  private DataflowActivityService activityService ;
  private Map<String, DataflowWorkerHeartbeatListener> workerHeartbeatListeners = new HashMap<>();
  
  public DataflowWorkerMonitor(DataflowRegistry dflRegistry, DataflowActivityService activityService) throws RegistryException {
    this.dataflowRegistry = dflRegistry;
    this.activityService = activityService ;
  }

  synchronized public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    DataflowWorkerHeartbeatListener listener = new DataflowWorkerHeartbeatListener (vmDescriptor) ;
    workerHeartbeatListeners.put(vmDescriptor.getRegistryPath(), listener);
  }
  
  synchronized void removeWorkerListener(String heartbeatPath) throws Exception {
    Node heartbeatNode = new Node(dataflowRegistry.getRegistry(), heartbeatPath);
    Node vmNode = heartbeatNode.getParentNode().getParentNode();
    workerHeartbeatListeners.remove(vmNode.getPath());
    Node dataflowWorkerNode = dataflowRegistry.getActiveWorkersNode().getChild(vmNode.getName());
    DataflowWorkerStatus dataflowWorkerStatus = dataflowWorkerNode.getChild("status").getDataAs(DataflowWorkerStatus.class);
    dataflowRegistry.historyWorker(vmNode.getName());
    
    System.err.println(">>>DataflowWorkerStatus = " + dataflowWorkerStatus) ;
    
    if(dataflowWorkerStatus != DataflowWorkerStatus.TERMINATED) {
      AddWorkerActivityBuilder addWorkerActivityBuilder =  new AddWorkerActivityBuilder(1);
      ActivityCoordinator addWorkerCoordinator = activityService.start(addWorkerActivityBuilder);
    }
  }
  
  public class DataflowWorkerHeartbeatListener extends NodeEventWatcher {
    public DataflowWorkerHeartbeatListener(VMDescriptor vmDescriptor) throws RegistryException {
      super(dataflowRegistry.getRegistry(), true);
      watchExists(vmDescriptor.getRegistryPath() + "/status/heartbeat");
    }
    
    @Override
    public void processNodeEvent(NodeEvent nodeEvent) throws Exception {
      if(nodeEvent.getType() == NodeEvent.Type.DELETE) {
        removeWorkerListener(nodeEvent.getPath());
        setComplete();
      }
    }
  }
}