package com.neverwinterdp.scribengin.dataflow.service;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.vm.VMDescriptor;

public class DataflowWorkerMonitor {
  private DataflowRegistry dataflowRegistry ;
  private Map<String, DataflowWorkerHeartbeatListener> workerHeartbeatListeners = new HashMap<>();
  
  public DataflowWorkerMonitor(DataflowRegistry dflRegistry) throws RegistryException {
    this.dataflowRegistry = dflRegistry;
  }

  synchronized public void addWorker(VMDescriptor vmDescriptor) throws RegistryException {
    DataflowWorkerHeartbeatListener listener = new DataflowWorkerHeartbeatListener (vmDescriptor) ;
    workerHeartbeatListeners.put(vmDescriptor.getRegistryPath(), listener);
  }
  
  synchronized void removeWorkerListener(String heartbeatPath) throws RegistryException {
    Node heartbeatNode = new Node(dataflowRegistry.getRegistry(), heartbeatPath);
    Node vmNode = heartbeatNode.getParentNode().getParentNode();
    workerHeartbeatListeners.remove(vmNode.getPath());
    // VMDescriptor vmDescriptor = vmNode.getDataAs(VMDescriptor.class) ;
    dataflowRegistry.historyWorker(vmNode.getName());
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