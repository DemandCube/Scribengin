package com.neverwinterdp.scribengin.junit;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.junit.AssertEvent;

public class ScribenginAssertEvent extends AssertEvent {
  static public enum ScribenginAttr { master_leader}
  
  final static public String SCRIBENGIN_MASTER_ELECTION = "scribengin-master-election" ;
  
  public ScribenginAssertEvent(String name, NodeEvent event) {
    super(name, event);
  }
  
  public void attr(ScribenginAttr attr, Object value) {
    attr(attr.toString(), value);
  }
  
  public <T> T attr(ScribenginAttr attr) {
    return (T) attr(attr.toString());
  }
}