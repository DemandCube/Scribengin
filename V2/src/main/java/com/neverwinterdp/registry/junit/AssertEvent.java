package com.neverwinterdp.registry.junit;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.registry.NodeEvent;

public class AssertEvent {
  private String    name ;
  private NodeEvent nodeEvent;
  private Map<String, Object> attributes = new HashMap<String, Object>() ;
  
  public AssertEvent(NodeEvent nodeEvent) {
    this.nodeEvent = nodeEvent;
  }

  public String getName() { return this.name; }
  
  public NodeEvent getNodeEvent() { return nodeEvent; }
  
  public <T> T attr(String name) {
    Object val = attributes.get(name);
    return (T) val;
  }
  
  public <T> T attr(Class<T> type) {
    Object val = attributes.get(type.getName());
    return (T) val;
  }
  
  public void attr(String name, Object value) {
    attributes.put(name, value);
  }
  
  public void attr(Class<?> type, Object value) {
    attributes.put(type.getName(), value);
  }
}
