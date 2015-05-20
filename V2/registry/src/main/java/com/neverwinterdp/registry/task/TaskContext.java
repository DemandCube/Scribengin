package com.neverwinterdp.registry.task;

import com.neverwinterdp.registry.RegistryException;

public class TaskContext<T> {
  private TaskTransactionId taskTransactionId;
  private T                 taskDescriptor;
  private TaskStatus        taskStatus;
  private TaskRegistry<T>   taskRegistry;

  public TaskContext(TaskRegistry<T> taskRegistry, String taskTransactionId, T taskDescriptor) {
    this(taskRegistry, new TaskTransactionId(taskTransactionId), taskDescriptor) ;
  }
  
  public TaskContext(TaskRegistry<T> taskRegistry, TaskTransactionId taskTransactionId, T taskDescriptor) {
    this.taskRegistry = taskRegistry;
    this.taskTransactionId = taskTransactionId;
    this.taskDescriptor = taskDescriptor;
  }

  public TaskTransactionId getTaskTransactionId() { return taskTransactionId; }

  public TaskRegistry<T> getTaskRegistry() { return this.taskRegistry; }
  
  public T getTaskDescriptor(boolean reload) throws RegistryException { 
    if(taskDescriptor == null || reload) taskDescriptor = taskRegistry.getTaskDescriptor(taskTransactionId.getTaskId()) ;
    return taskDescriptor; 
  }

  public TaskStatus getTaskStatus(boolean reload) throws RegistryException { 
    if(taskStatus == null || reload) taskStatus = taskRegistry.getTaskStatus(taskTransactionId.getTaskId()) ;
    return taskStatus; 
  }
  
  public void suspend(String executorRef, boolean disconnectHeartbeat) throws RegistryException {
    taskRegistry.suspend(executorRef, taskTransactionId, disconnectHeartbeat);
  }
}
