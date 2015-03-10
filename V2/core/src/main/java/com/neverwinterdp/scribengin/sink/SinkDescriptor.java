package com.neverwinterdp.scribengin.sink;

import java.util.HashMap;

public class SinkDescriptor extends HashMap<String, String>{
  
  public SinkDescriptor() {}
  
  public SinkDescriptor(String type) {
    setType(type);
  }
  
  public SinkDescriptor(String type, String location) {
    setType(type);
    setLocation(location) ;
  }
  
  public String getType() { return get("type"); }
  public void setType(String type) { put("type", type); }
  
  public String getLocation() { return get("location"); }
  public void   setLocation(String location) { put("location", location); }
  
  public String attribute(String name) {
    return get(name);
  }
  
  public void attribute(String name, String value) {
    put(name, value);
  }
  
  public void attribute(String name, int value) {
    put(name, Integer.toString(value));
  }
  
  public int intAttribute(String name) {
    String value = get(name);
    if(value == null) return 0;
    return Integer.parseInt(value);
  }
}