package com.neverwinterdp.scribengin.tuple;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;

public class Tuple {
  private String key ;
  private byte[] data ;
  private CommitLogEntry commitLogEntry;
  private boolean invalidData;
  
  public Tuple() {} 
  
  public Tuple(String key, byte[] data, CommitLogEntry c) {
    this.key = key ;
    this.data = data ;
    this.setCommitLogEntry(c);
    this.setInvalidData(false);
  }
  
  public String getKey() { return key; }
  public void setKey(String key) { this.key = key; }
  
  public byte[] getData() { return data; }
  public void setData(byte[] data) { this.data = data; }

  public CommitLogEntry getCommitLogEntry() {
    return this.commitLogEntry;
  }

  public void setCommitLogEntry(CommitLogEntry c) {
    this.commitLogEntry = c;
  }

  public boolean isInvalidData() {
    return this.invalidData;
  }

  public void setInvalidData(boolean isInvalid) {
    this.invalidData = isInvalid;
    this.commitLogEntry.setInvalidData(isInvalid);
  }
}
