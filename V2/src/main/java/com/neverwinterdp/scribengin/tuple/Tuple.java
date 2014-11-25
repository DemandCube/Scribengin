package com.neverwinterdp.scribengin.tuple;

import java.util.Arrays;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;

public class Tuple {
  private String key ;
  private byte[] data ;
  private CommitLogEntry commitLogEntry;
  private boolean invalidData;
  private boolean taskGenerated;
  
  public Tuple() {} 
  
  public Tuple(String key, byte[] data, CommitLogEntry c) {
    this.key = key ;
    this.data = data ;
    this.setCommitLogEntry(c);
    this.invalidData = false;
    this.setTaskGenerated(false);
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
        t.isInvalidData() == this.isInvalidData() &&
        t.isTaskGenerated() == this.isTaskGenerated()){
      return true;
    }
    return false;
  }
  
  @Override 
  public String toString() {
    StringBuilder result = new StringBuilder();
    String NEW_LINE = System.getProperty("line.separator");
    result.append("key: ");
    result.append(this.key);
    result.append(NEW_LINE);
    
    result.append("data: ");
    result.append(new String(this.data));
    result.append(NEW_LINE);
    
    result.append("invalidData: ");
    result.append(String.valueOf(this.invalidData));
    result.append(NEW_LINE);
    
    result.append("taskGenerated: ");
    result.append(String.valueOf(this.taskGenerated));
    result.append(NEW_LINE);
    
    result.append("commitLog: ");
    result.append(NEW_LINE);
    String[] commitLog = this.commitLogEntry.toString().split(NEW_LINE);
    for(String x: commitLog){
      result.append("\t");
      result.append(x);
      result.append(NEW_LINE);
    }
    
    

    return result.toString();
  }

  public boolean isTaskGenerated() {
    return taskGenerated;
  }

  public void setTaskGenerated(boolean taskGenerated) {
    this.taskGenerated = taskGenerated;
  }
}
