package com.neverwinterdp.registry.task;

import com.neverwinterdp.registry.Node;

public class TaskContext<T> {
  private String taskId ;
  private T      taskDescriptor ;
  private Node   taskNode ;
  
  public TaskContext(String taskId, T taskDescriptor, Node taskNode) {
    this.taskId = taskId;
    this.taskDescriptor = taskDescriptor;
    this.taskNode = taskNode; 
  }

  public String getTaskId() { return taskId; }

  public T getTaskDescriptor() { return taskDescriptor; }

  public Node getTaskNode() { return taskNode; }
}
