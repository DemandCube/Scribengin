package com.neverwinterdp.scribengin.dataflow;

public class DataflowTaskReport {
  private long startTime  ;
  private long finishTime ;
  private long processCount ;
  private long commitProcessCount;
  
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
