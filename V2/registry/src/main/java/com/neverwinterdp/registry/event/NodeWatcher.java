package com.neverwinterdp.registry.event;

abstract public class NodeWatcher {
  private boolean complete = false;
  
  public boolean isComplete() { return complete; }
  public void setComplete() { complete = true; }
  
  abstract public void onEvent(NodeEvent event) throws Exception ;
}