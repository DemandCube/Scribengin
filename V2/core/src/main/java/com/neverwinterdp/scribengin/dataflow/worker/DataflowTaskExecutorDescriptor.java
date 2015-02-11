package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.ArrayList;
import java.util.List;

import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;


public class DataflowTaskExecutorDescriptor {
  public static enum Status { INIT, RUNNING, TERMINATED }
  
  private String id ;
  private Status status = Status.INIT ;
  private List<Integer> assignedTaskIds = new ArrayList<>() ;
  
  public DataflowTaskExecutorDescriptor() { }

  public DataflowTaskExecutorDescriptor(String id) {
    this.id  = id ;
  }
  
  public String getId() { return this.id ; }
  public void setId(String id) { this.id = id ; }
  
  public Status getStatus() { return status ; }
  public void setStatus(Status status) { this.status = status ; }

  public List<Integer> getAssignedTaskIds() { return assignedTaskIds; }
  public void setAssignedTaskIds(List<Integer> assignedTasks) {
    this.assignedTaskIds = assignedTasks;
  }
  
  public void addAssignedTask(DataflowTaskDescriptor taskDescriptor) {
    assignedTaskIds.add(taskDescriptor.getId());
  }

}
