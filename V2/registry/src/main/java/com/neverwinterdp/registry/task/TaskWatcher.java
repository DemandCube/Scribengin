package com.neverwinterdp.registry.task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.notification.Notifier;

class TaskWatcher<T> {
  private AddRemoveNodeChildrenWatcher<T> availableTaskWatcher;
  private AddRemoveNodeChildrenWatcher<T> assignedTaskWatcher;
  private AddRemoveNodeChildrenWatcher<T> finishedTaskWatcher;
  
  private TaskRegistry<T> taskRegistry ;
  private List<TaskMonitor<T>> taskMonitors = new ArrayList<TaskMonitor<T>>();
  private LinkedBlockingQueue<TaskOperation<T>> taskOperationQueue = new LinkedBlockingQueue<TaskOperation<T>>() ;
  private TaskOperationExecutor taskOperationExecutor ;
  
  public TaskWatcher(TaskRegistry<T> tRegistry) throws RegistryException {
    this.taskRegistry = tRegistry;
    Registry registry = tRegistry.getRegistry() ;
    availableTaskWatcher = new AddRemoveNodeChildrenWatcher<T>(registry, tRegistry.getTasksAvailableNode()) {
      @Override
      public void onAddChild(String taskId) {
        enqueue(new OnAvailableTaskOperation(), taskId);
      }
    };
    assignedTaskWatcher = new AddRemoveNodeChildrenWatcher<T>(registry, tRegistry.getTasksAssignedNode()) {
      @Override
      public void onAddChild(String taskId) {
        enqueue(new OnTakeTaskOperation(), taskId);
      }
    };
    finishedTaskWatcher = new AddRemoveNodeChildrenWatcher<T>(registry, tRegistry.getTasksFinishedNode()) {
      @Override
      public void onAddChild(String taskId) {
        enqueue(new OnFinishTaskOperation(), taskId);
      }
    };
    taskOperationExecutor = new TaskOperationExecutor() ;
    taskOperationExecutor.start();
  }
  
  public List<TaskMonitor<T>> getTaskMonitors() { return this.taskMonitors; }
  
  public void addTaskMonitor(TaskMonitor<T> monitor) {
    taskMonitors.add(monitor);
  }
  
  public void onDestroy() {
    availableTaskWatcher.setComplete();
    assignedTaskWatcher.setComplete();
    finishedTaskWatcher.setComplete();
    if(taskOperationExecutor!= null && taskOperationExecutor.isAlive()) {
      taskOperationExecutor.interrupt();
    }
  }
  
  void enqueue(TaskOperation<T> op, String taskId) {
    try {
      op.init(taskRegistry.createTaskContext(taskId)) ;
      taskOperationQueue.offer(op) ;
    } catch (RegistryException e) {
      Notifier notifier =  taskRegistry.getTaskCoordinationNotifier();
      try {
        notifier.error("error-enqueue-a-task-monitor-operation", "Error when enqueue a task monitor operation", e);
      } catch (RegistryException e1) {
        e1.printStackTrace();
      }
    }
  }
  
  static abstract public class TaskOperation<T> {
    protected TaskContext<T> taskContext ;
    
    public TaskOperation<T> init(TaskContext<T> taskContext) {
      this.taskContext = taskContext;
      return this;
    }
    
    abstract public void execute() ;
  }
  
  class  OnTakeTaskOperation extends TaskOperation<T> {
    public void execute() {
      for(TaskMonitor<T> sel : taskMonitors) {
        sel.onAssign(taskContext);
      }
    }
  }
  
  class  OnFinishTaskOperation extends TaskOperation<T> {
    public void execute() {
      for(TaskMonitor<T> sel : taskMonitors) {
        sel.onFinish(taskContext);
      }
    }
  }
  
  class  OnAvailableTaskOperation extends TaskOperation<T> {
    public void execute() {
      for(TaskMonitor<T> sel : taskMonitors) {
        sel.onAvailable(taskContext);
      }
    }
  }
  
  class TaskOperationExecutor extends Thread {
    public void run() {
      try {
        while(true) {
          TaskOperation<T> taskOperation = taskOperationQueue.take();
          taskOperation.execute();
        }
      } catch (InterruptedException e) {
      }
    }
  }
}