package com.neverwinterdp.scribengin.stream.streamdescriptor;

public class JsonSourceStreamDescriptor extends OffsetStreamDescriptor{
  private String key;
  
  public JsonSourceStreamDescriptor(String name, String key){
    super(name, 0, 0);
    this.setKey("data");
  }
  
  public JsonSourceStreamDescriptor(String name, int lastCommittedOffset,
      int currentOffset) {
    super(name, lastCommittedOffset, currentOffset);
    this.setKey("data");
  }
  
  public JsonSourceStreamDescriptor(String name, int lastCommittedOffset,
      int currentOffset, String key) {
    super(name, lastCommittedOffset, currentOffset);
    this.setKey(key);
  }
  
  

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

}
