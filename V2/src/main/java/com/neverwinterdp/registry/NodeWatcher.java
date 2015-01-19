package com.neverwinterdp.registry;

abstract public class NodeWatcher {
  private boolean complete = false;
  
  public boolean isComplete() { return complete; }
  
  public void setComplete() { complete = true; }
  
  abstract public void process(NodeEvent event) ;
}
