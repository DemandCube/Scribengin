package com.neverwinterdp.scribengin.commitlog;

public class CommitLog {
  private String readerName ;
  private long   startOffset ;
  private long   endOffset   ;
  
  public CommitLog() {} 
  
  public CommitLog(String readerName, long startOffset, long endOffset) {
    this.readerName = readerName ;
    this.startOffset = startOffset ;
    this.endOffset = endOffset ;
  }
  
  public String getDatasourceName() { return readerName; }
  public void setDatasourceName(String readerName) {
    this.readerName = readerName;
  }
  
  public long getStartOffset() { return startOffset; }
  public void setStartOffset(long startOffset) { this.startOffset = startOffset; }
  
  public long getEndOffset() { return endOffset; }
  public void setEndOffset(long endOffset) { this.endOffset = endOffset; }
}
