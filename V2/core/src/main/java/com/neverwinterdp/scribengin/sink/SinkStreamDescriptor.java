package com.neverwinterdp.scribengin.sink;

import java.util.Map;

public class SinkStreamDescriptor extends SinkDescriptor {
  public SinkStreamDescriptor() { 
  }
  
  public SinkStreamDescriptor(Map<String, String> props) { 
    putAll(props);
  }
  
  public SinkStreamDescriptor(String type, int id, String location) {
    super(type, location);
    setId(id);
  }
  
  public int getId() { return intAttribute("id"); }
  public void setId(int id) { attribute("id", id); }

  
}
