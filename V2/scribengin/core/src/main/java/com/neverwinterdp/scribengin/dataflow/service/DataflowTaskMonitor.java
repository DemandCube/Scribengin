package com.neverwinterdp.scribengin.dataflow.service;

import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;
import com.neverwinterdp.registry.task.TaskContext;
import com.neverwinterdp.registry.task.TaskMonitor;
import com.neverwinterdp.registry.task.TaskRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;

public class DataflowTaskMonitor implements TaskMonitor<DataflowTaskDescriptor> {

  @Override
  public void onAssign(TaskContext<DataflowTaskDescriptor> context) {
  }

  @Override
  public void onAvailable(TaskContext<DataflowTaskDescriptor> context) {
  }

  @Override
  public void onFinish(TaskContext<DataflowTaskDescriptor> context) {
    TaskRegistry<DataflowTaskDescriptor> taskRegistry = context.getTaskRegistry();
    try {
      int allTask = taskRegistry.getTasksListNode().getChildren().size();
      int finishTask = taskRegistry.getTasksFinishedNode().getChildren().size();
      if(allTask == finishTask) {
        synchronized(this) {
          notifyAll() ;
        }
      }
    } catch(Exception ex) {
      Notifier notifier = taskRegistry.getTaskCoordinationNotifier();
      try {
        notifier.error("fail-coodinate-a-finish-task", "Cannot coordinate a finished task", ex);
      } catch (RegistryException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void onFail(TaskContext<DataflowTaskDescriptor> context) {
    TaskRegistry<DataflowTaskDescriptor> taskRegistry = context.getTaskRegistry();
    try {
      context.suspend("DataflowService", true);
    } catch (RegistryException ex) {
      Notifier notifier = taskRegistry.getTaskCoordinationNotifier();
      try {
        notifier.error("fail-coodinate-a-fail-task", "Cannot coordinate a faild task", ex);
      } catch (RegistryException e) {
        e.printStackTrace();
      }
    }
  }

  synchronized public void waitForAllTaskFinish() throws InterruptedException {
    wait() ;
  }
  
}
