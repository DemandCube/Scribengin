package com.neverwinterdp.scribengin;

/**
 * @author Tuan Nguyen
 */
public class Record {
  private String key ;
  private byte[] data ;
  
  public Record() {} 
  
  public Record(String key, byte[] data) {
    this.key = key ;
    this.data = data ;
  }
  
  public String getKey() { return key; }
  public void setKey(String key) { this.key = key; }
  
  public byte[] getData() { return data; }
  public void setData(byte[] data) { this.data = data; }
}
