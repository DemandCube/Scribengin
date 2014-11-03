package com.neverwinterdp.scribengin.source;

public class CommitPoint {
  private String datasourceName ;
  private long   startOffset ;
  private long   endOffset   ;
  
  public CommitPoint() {} 
  
  public CommitPoint(String datasourceName, long startOffset, long endOffset) {
    this.datasourceName = datasourceName ;
    this.startOffset = startOffset ;
    this.endOffset = endOffset ;
  }
  
  public String getDatasourceName() { return datasourceName; }
  public void setDatasourceName(String datasourceName) {
    this.datasourceName = datasourceName;
  }
  
  public long getStartOffset() { return startOffset; }
  public void setStartOffset(long startOffset) { this.startOffset = startOffset; }
  
  public long getEndOffset() { return endOffset; }
  public void setEndOffset(long endOffset) { this.endOffset = endOffset; }
}
