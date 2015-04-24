package com.neverwinterdp.scribengin.dataflow.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeChildrenWatcher;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.NodeEventWatcher;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;

public class DataflowTaskMonitor extends NodeChildrenWatcher {
  private DataflowRegistry dataflowRegistry ;
  private Map<String, DataflowTaskHeartbeatListener> assignedTaskListeners = new HashMap<>();
  private int finishedTaskCount = 0 ;
  private int numOfTasks = 0;
  
  public DataflowTaskMonitor(DataflowRegistry dflRegistry) throws RegistryException {
    super(dflRegistry.getRegistry(), true);
    this.dataflowRegistry = dflRegistry;
    finishedTaskCount = dflRegistry.getTasksFinishedNode().getChildren().size() ;
    numOfTasks = dflRegistry.getTaskDescriptors().size();
    watchChildren(dflRegistry.getTasksAssignedPath());
  }
  
  
  public void addMonitorTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    numOfTasks++;
  }

  @Override
  public void processNodeEvent(NodeEvent nodeEvent) throws Exception {
    if(nodeEvent.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
      onChildrenChange(nodeEvent) ;
    } else if(nodeEvent.getType() == NodeEvent.Type.DELETE) {
    } else {
      System.err.println("unhandle assigned dataflow task event: " + nodeEvent.getPath() + " - " + nodeEvent.getType());
    }
  }
  
  synchronized public void onChildrenChange(NodeEvent event) {
    try {
      Registry registry = getRegistry();
      List<String> assignedTaskNames = registry.getChildren(dataflowRegistry.getTasksAssignedPath());
      for(String taskName : assignedTaskNames) {
        if(assignedTaskListeners.containsKey(taskName)) continue;
        addHeartbeatListener(taskName);
      }
    } catch (RegistryException e) {
      e.printStackTrace();
    }
  }
  
  synchronized void addHeartbeatListener(String taskName) throws RegistryException {
    DataflowTaskHeartbeatListener heartbeatListener = new DataflowTaskHeartbeatListener(dataflowRegistry, taskName);
    assignedTaskListeners.put(taskName, heartbeatListener);
  }
  
  synchronized void removeHeartbeatListener(String heartbeatPath) throws RegistryException {
    Node heartbeatNode = new Node(getRegistry(), heartbeatPath);
    Node assignedTaskNode = heartbeatNode.getParentNode();
    assignedTaskListeners.remove(assignedTaskNode.getName());
    DataflowTaskDescriptor descriptor = dataflowRegistry.getTaskDescriptor(assignedTaskNode.getName());
    DataflowTaskDescriptor.Status status = descriptor.getStatus();
    if(status != DataflowTaskDescriptor.Status.SUSPENDED && status != DataflowTaskDescriptor.Status.TERMINATED) {
      onFailDataflowTask(descriptor);
    } else if(status == DataflowTaskDescriptor.Status.TERMINATED) {
      onFinishDataflowTask(descriptor);
    }
  }
  
  public void onFailDataflowTask(DataflowTaskDescriptor descriptor) throws RegistryException {
    DataflowTaskDescriptor.Status status = descriptor.getStatus();
    dataflowRegistry.dataflowTaskSuspend(descriptor);
  }
  
  synchronized public void onFinishDataflowTask(DataflowTaskDescriptor descriptor) throws RegistryException {
    DataflowTaskDescriptor.Status status = descriptor.getStatus();
    finishedTaskCount++ ;
    System.err.println("Detect finish task with status = " + status + " finishedTaskCount = " + finishedTaskCount +", numOfTasks = " + numOfTasks);
    if(numOfTasks == finishedTaskCount) {
      notifyAll() ;
    }
  }
 
  synchronized public void waitForAllTaskFinish() throws InterruptedException {
    wait() ;
  }
  
  public class DataflowTaskHeartbeatListener extends NodeEventWatcher {
    public DataflowTaskHeartbeatListener(DataflowRegistry dataflowRegistry, String taskName) throws RegistryException {
      super(dataflowRegistry.getRegistry(), true);
      String heartbeatPath = dataflowRegistry.getTasksAssignedPath() + "/" + taskName + "/heartbeat";
      watchExists(heartbeatPath);
    }
    
    @Override
    public void processNodeEvent(NodeEvent nodeEvent) throws Exception {
      if(nodeEvent.getType() == NodeEvent.Type.DELETE) {
        removeHeartbeatListener(nodeEvent.getPath());
        setComplete();
      }
    }
  }
}
