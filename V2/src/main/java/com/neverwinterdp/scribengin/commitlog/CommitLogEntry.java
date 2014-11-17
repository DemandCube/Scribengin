package com.neverwinterdp.scribengin.commitlog;

public class CommitLogEntry {
  private String readerName ;
  private long   startOffset ;
  private long   endOffset   ;
  private boolean invalidData ; 
  
  public CommitLogEntry() {} 
  
  public CommitLogEntry(String readerName, long startOffset, long endOffset) {
    this.readerName = readerName ;
    this.startOffset = startOffset ;
    this.endOffset = endOffset ;
    this.invalidData = false;
  }
  
  public CommitLogEntry(String readerName, long startOffset, long endOffset, boolean invalidData) {
    this.readerName = readerName ;
    this.startOffset = startOffset ;
    this.endOffset = endOffset ;
    this.invalidData = invalidData;
  }
  
  public String getDatasourceName() { return readerName; }
  public void setDatasourceName(String readerName) {
    this.readerName = readerName;
  }
  
  public long getStartOffset() { return startOffset; }
  public void setStartOffset(long startOffset) { this.startOffset = startOffset; }
  
  public long getEndOffset() { return endOffset; }
  public void setEndOffset(long endOffset) { this.endOffset = endOffset; }

  public boolean isInvalidData() {
    return invalidData;
  }

  public void setInvalidData(boolean invalidData) {
    this.invalidData = invalidData;
  }
}
