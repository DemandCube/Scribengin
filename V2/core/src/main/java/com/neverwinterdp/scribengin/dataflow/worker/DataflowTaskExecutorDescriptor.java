package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;


public class DataflowTaskExecutorDescriptor {
  public static enum Status { INIT, RUNNING, TERMINATED }
  
  private String id ;
  private Status status = Status.INIT ;
  private List<String> assignedTaskIds = new ArrayList<>() ;
  
  public DataflowTaskExecutorDescriptor() { }

  public DataflowTaskExecutorDescriptor(String id) {
    this.id  = id ;
  }
  
  public String getId() { return this.id ; }
  public void setId(String id) { this.id = id ; }
  
  public Status getStatus() { return status ; }
  public void setStatus(Status status) { this.status = status ; }

  public List<String> getAssignedTaskIds() { return assignedTaskIds; }
  public void setAssignedTaskIds(List<String> assignedTasks) {
    this.assignedTaskIds = assignedTasks;
  }
  
  public void addAssignedTask(String taskId) {
    assignedTaskIds.add(taskId);
  }

}
