package com.neverwinterdp.scribengin.dataflow.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEventListener;
import com.neverwinterdp.registry.event.NodeChildrenListener;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.event.ScribenginEvent;

public class AssignedDataflowTaskListener extends NodeChildrenListener<ScribenginEvent> {
  private Map<String, DataflowTaskHeartbeatListener> assignedTaskListeners = new HashMap<>();
  private DataflowRegistry dataflowRegistry ;
  
  public AssignedDataflowTaskListener(DataflowRegistry dataflowRegistry) throws RegistryException {
    super(dataflowRegistry.getRegistry(), true);
    this.dataflowRegistry = dataflowRegistry;
    watchChildren(dataflowRegistry.getTasksAssignedPath());
  }

  @Override
  public ScribenginEvent toAppEvent(Registry registry, NodeEvent nodeEvent) throws Exception {
    ScribenginEvent appEvent = new ScribenginEvent("assigned-tasks", nodeEvent);
    return appEvent;
  }

  @Override
  public void onEvent(ScribenginEvent event) throws Exception {
    NodeEvent nodeEvent = event.getNodeEvent();
    if(nodeEvent.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
      onChildrenChange(event) ;
    } else {
      System.err.println("unhandle assigned dataflow task event: " + nodeEvent.getPath() + " - " + nodeEvent.getType());
    }
  }
  
  synchronized public void onChildrenChange(ScribenginEvent event) {
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
      dataflowRegistry.dataflowTaskSuspend(descriptor);
      System.err.println("    detect unfinished task: " + assignedTaskNode.getName() + ", status = " + status);
    }
  }
  
  public class DataflowTaskHeartbeatListener extends NodeEventListener<ScribenginEvent> {
    public DataflowTaskHeartbeatListener(DataflowRegistry dataflowRegistry, String taskName) throws RegistryException {
      super(dataflowRegistry.getRegistry(), true);
      String hearbeatPath = dataflowRegistry.getTasksAssignedPath() + "/" + taskName + "/heartbeat";
      watchExists(hearbeatPath);
    }
    
    @Override
    public ScribenginEvent toAppEvent(Registry registry, NodeEvent nodeEvent) throws Exception {
      ScribenginEvent appEvent = new ScribenginEvent("assigned-task-heartbeat", nodeEvent);
      return appEvent;
    }

    @Override
    public void onEvent(ScribenginEvent event) throws Exception {
      NodeEvent nodeEvent = event.getNodeEvent();
      if(nodeEvent.getType() == NodeEvent.Type.DELETE) {
        removeHeartbeatListener(nodeEvent.getPath());
        setComplete();
      }
    }
  }
}
