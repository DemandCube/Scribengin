package com.neverwinterdp.registry.task;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class TaskTransactionId {
  private String taskId ;
  private String transactionId ;
  
  public TaskTransactionId() {} 
  
  public TaskTransactionId(String taskId, String transactionId) {
    this.taskId = taskId ;
    this.transactionId = transactionId ;
  }
  
  public TaskTransactionId(String id) {
    int txIdx = id.indexOf("-tx-") ; 
    if(txIdx > 0) {
      this.taskId = id.substring(0, txIdx);
      this.transactionId = id.substring(txIdx + 4, id.length()) ;
    } else {
      taskId = id;
    }
  }
  
  public String getTaskId() { return taskId; }
  public void setTaskId(String taskId) { this.taskId = taskId; }
  
  public String getTransactionId() { return transactionId; }
  public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

  @JsonIgnore
  public String getTaskTransactionId() { 
    if(transactionId == null) return taskId ;
    return taskId + "-tx-" + transactionId ;
  }
  
  public String toString() {
    return "taskId = " + taskId + ", transactionId = " + transactionId ;
  }
}
