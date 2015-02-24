package com.neverwinterdp.scribengin.event;

import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.registry.event.Event;

public class ScribenginEvent extends Event {
  static public enum DataflowAttr   { status, descriptor }
  static public enum ScribenginAttr { dataflow_status, dataflow_descriptor }
  
  final static public String DATAFLOW_STATUS = "dataflow-status" ;
  
  public ScribenginEvent(String name, NodeEvent event) {
    super(name, event);
  }
  
  public void attr(ScribenginAttr attr, Object value) {
    attr(attr.toString(), value);
  }

  public <T> T attr(ScribenginAttr attr) {
    return (T) attr(attr.toString());
  }
  
  public void attr(DataflowAttr attr, Object value) {
    attr(attr.toString(), value);
  }
  
  public <T> T attr(DataflowAttr attr) {
    return (T) attr(attr.toString());
  }
}