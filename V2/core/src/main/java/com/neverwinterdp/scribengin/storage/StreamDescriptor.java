package com.neverwinterdp.scribengin.storage;

import java.util.Map;

public class StreamDescriptor extends StorageDescriptor {

  public StreamDescriptor() {
  }
  
  public StreamDescriptor(String type, int id, String location) {
    setType(type);
    setId(id);
    setLocation(location);
  }
  
  public StreamDescriptor(Map<String, String> props) {
    putAll(props);
  }
  
  public int  getId() { return intAttribute("id"); }
  public void setId(int id) { attribute("id", id); }
  
}
