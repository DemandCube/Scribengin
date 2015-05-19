package com.neverwinterdp.scribengin.dataflow.service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeChildrenWatcher;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;

public class DataflowTaskMonitor {
  private DataflowRegistry dataflowRegistry ;
  private AssignedDataflowTaskHeartbeatWatcher assignedDataflowTaskHeartbeatWatcher ;
  private FinishDataflowTaskWatcher finishDataflowTaskWatcher;
  private int numOfTasks = 0;
  
  public DataflowTaskMonitor(DataflowRegistry dflRegistry) throws RegistryException {
    this.dataflowRegistry = dflRegistry;
    numOfTasks = dflRegistry.getTaskDescriptors().size();
    
    assignedDataflowTaskHeartbeatWatcher = new AssignedDataflowTaskHeartbeatWatcher(dflRegistry.getRegistry(), true);
    assignedDataflowTaskHeartbeatWatcher.watchChildren(dflRegistry.getTasksAssignedHeartbeatNode().getPath());
    
    finishDataflowTaskWatcher = new FinishDataflowTaskWatcher(dflRegistry.getRegistry(), true);
    finishDataflowTaskWatcher.watchChildren(dflRegistry.getTasksFinishedNode().getPath());
  }
  
  public void addMonitorTask(DataflowTaskDescriptor taskDescriptor) throws RegistryException {
    numOfTasks++;
  }

  synchronized void onDeleteHeartbeat(String taskId) throws RegistryException {
    DataflowTaskDescriptor descriptor = dataflowRegistry.getTaskDescriptor(taskId);
    DataflowTaskDescriptor.Status status = descriptor.getStatus();
    if(status != DataflowTaskDescriptor.Status.SUSPENDED || status != DataflowTaskDescriptor.Status.TERMINATED) {
      Notifier notifier = dataflowRegistry.getDataflowTaskNotifier() ;
      notifier.warn("detect-failed-dataflow-" + taskId, "Detect the failed dataflow task " + taskId + ", move the task to suspended");
      dataflowRegistry.dataflowTaskSuspend(descriptor, true);
    } else if(status == DataflowTaskDescriptor.Status.TERMINATED) {
      onFinishDataflowTask();
    }
  }
  
  synchronized public void onFinishDataflowTask() throws RegistryException {
    if(numOfTasks == dataflowRegistry.getTasksFinishedNode().getChildren().size()) {
      notifyAll() ;
    }
  }
 
  synchronized public void waitForAllTaskFinish() throws InterruptedException {
    wait() ;
  }
  
  synchronized void processHeartbeatNodeEvent(NodeEvent nodeEvent) throws Exception {
    if(nodeEvent.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
      List<String> assignedTaskHeartbeats = dataflowRegistry.getTasksAssignedHeartbeatNode().getChildren();
      List<String> assignedTasks = dataflowRegistry.getTasksAssignedNode().getChildren();
      Set<String> assignedTaskHeartbeatSet = new HashSet<>();
      assignedTaskHeartbeatSet.addAll(assignedTaskHeartbeats);
      for(int i = 0; i < assignedTasks.size(); i++) {
        String assignedTask = assignedTasks.get(i) ;
        if(!assignedTaskHeartbeatSet.contains(assignedTask)) {
          onDeleteHeartbeat(assignedTask);
        }
      }
    } else if(nodeEvent.getType() == NodeEvent.Type.DELETE) {
    } else {
      System.err.println("unhandle assigned dataflow task event: " + nodeEvent.getPath() + " - " + nodeEvent.getType());
    }
  }
  
  public  class FinishDataflowTaskWatcher extends NodeChildrenWatcher {
    public FinishDataflowTaskWatcher(Registry registry, boolean persistent) {
      super(registry, persistent);
    }
    
    @Override
    public void processNodeEvent(NodeEvent nodeEvent) throws Exception {
      if(nodeEvent.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
        onFinishDataflowTask();
      } else if(nodeEvent.getType() == NodeEvent.Type.DELETE) {
      } else {
        System.err.println("unhandle assigned dataflow task event: " + nodeEvent.getPath() + " - " + nodeEvent.getType());
      }
    }
  }
  
  public  class AssignedDataflowTaskHeartbeatWatcher extends NodeChildrenWatcher {
    public AssignedDataflowTaskHeartbeatWatcher(Registry registry, boolean persistent) {
      super(registry, persistent);
    }
    
    @Override
    public void processNodeEvent(NodeEvent nodeEvent) throws Exception {
      processHeartbeatNodeEvent(nodeEvent);
    }
  }
}
