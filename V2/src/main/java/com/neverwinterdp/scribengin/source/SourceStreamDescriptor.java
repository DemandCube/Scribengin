package com.neverwinterdp.scribengin.source;

import java.util.Map;

public class SourceStreamDescriptor extends SourceDescriptor {

  public SourceStreamDescriptor() {
  }
  
  public SourceStreamDescriptor(Map<String, String> props) {
    putAll(props);
  }
  
  public int  getId() { return intAttribute("id"); }
  public void setId(int id) { attribute("id", id); }
  
}
