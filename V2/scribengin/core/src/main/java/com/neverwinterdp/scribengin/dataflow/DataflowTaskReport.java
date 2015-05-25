package com.neverwinterdp.scribengin.dataflow;

import java.util.Comparator;

public class DataflowTaskReport {
  final static public Comparator<DataflowTaskReport> COMPARATOR = new Comparator<DataflowTaskReport>() {
    @Override
    public int compare(DataflowTaskReport o1, DataflowTaskReport o2) {
      return o1.getTaskId().compareTo(o2.getTaskId());
    }
  };
  
  private String taskId ;
  private long  startTime  ;
  private long  finishTime ;
  private long  processCount ;
  private long  commitProcessCount;
  
  public DataflowTaskReport() {} 
  
  public DataflowTaskReport(String taskId) {
    this.taskId = taskId ;
    this.startTime = System.currentTimeMillis();
  }
  
  public String getTaskId() { return taskId; }
  public void setTaskId(String taskId) { this.taskId = taskId; }
  
  public long getStartTime() { return startTime; }
  public void setStartTime(long startTime) { this.startTime = startTime; }
  
  public long getFinishTime() { return finishTime; }
  public void setFinishTime(long finishTime) { this.finishTime = finishTime; }
  
  public long getProcessCount() { return processCount; }
  public void setProcessCount(long processCount) { this.processCount = processCount; }
  public void incrProcessCount() { processCount++ ; }
  
  public long getCommitProcessCount() { return commitProcessCount; }
  public void setCommitProcessCount(long commitProcessCount) { this.commitProcessCount = commitProcessCount; }
  public void incrCommitProcessCount() { 
    commitProcessCount += processCount ;
    processCount = 0;
  }
}
