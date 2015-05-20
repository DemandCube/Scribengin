package com.neverwinterdp.registry.task;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.RegistryException;

public class TaskContext<T> {
  private String          taskId;
  private T               taskDescriptor;
  private TaskStatus      taskStatus;
  private Node            taskNode;
  private TaskRegistry<T> taskRegistry;

  public TaskContext(String taskId, T taskDescriptor, Node taskNode) {
    this.taskId = taskId;
    this.taskDescriptor = taskDescriptor;
    this.taskNode = taskNode; 
  }

  public String getTaskId() { return taskId; }

  public T getTaskDescriptor(boolean reload) throws RegistryException { 
    if(taskDescriptor == null || reload) taskDescriptor = taskRegistry.getTaskDescriptor(taskId) ;
    return taskDescriptor; 
  }

  public TaskStatus getTaskStatus(boolean reload) throws RegistryException { 
    if(taskStatus == null || reload) taskStatus = taskRegistry.getTaskStatus(taskId) ;
    return taskStatus; 
  }
  
  public Node getTaskNode() { return taskNode; }
}
