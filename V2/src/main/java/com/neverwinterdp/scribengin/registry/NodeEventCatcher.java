package com.neverwinterdp.scribengin.registry;

public class NodeEventCatcher implements NodeWatcher {
  private NodeEvent nodeEvent ;
  
  public void process(NodeEvent event) {
    this.nodeEvent = event ;
  }
  
  public NodeEvent getNodeEvent() { return this.nodeEvent ; }
}
