package com.neverwinterdp.scribengin.junit;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.junit.AssertEvent;

public class ScribenginAssertEvent extends AssertEvent {
  static public enum DataflowAttr { status, descriptor }
  static public enum ScribenginAttr { dataflow_status, dataflow_descriptor }
  
  final static public String DATAFLOW_STATUS = "dataflow-status" ;
  
  public ScribenginAssertEvent(String name, NodeEvent event) {
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