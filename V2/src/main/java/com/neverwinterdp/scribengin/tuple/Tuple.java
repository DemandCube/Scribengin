package com.neverwinterdp.scribengin.tuple;

import java.util.Arrays;

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

  public boolean equals(Object obj) {
    if(!(obj instanceof Tuple)){
      return false;
    }
    if(obj == this){
      return true;
    }
    Tuple t = (Tuple) obj;
    if(Arrays.equals(t.getData(), this.getData()) &&
        t.getKey().equals(this.getKey()) &&
        t.getCommitLogEntry().equals(this.commitLogEntry) &&
        t.isInvalidData() == this.isInvalidData()){
      return true;
    }
    return false;
  }
}
