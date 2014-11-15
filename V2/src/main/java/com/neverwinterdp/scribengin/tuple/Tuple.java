package com.neverwinterdp.scribengin.tuple;

/**
 * @author Tuan Nguyen
 */
public class Tuple {
  private String key ;
  private byte[] data ;
  
  public Tuple() {} 
  
  public Tuple(String key, byte[] data) {
    this.key = key ;
    this.data = data ;
  }
  
  public String getKey() { return key; }
  public void setKey(String key) { this.key = key; }
  
  public byte[] getData() { return data; }
  public void setData(byte[] data) { this.data = data; }
}
