package com.neverwinterdp.registry.event;

abstract public class NodeWatcherWrapper extends NodeWatcher {
  protected NodeWatcher nodeWatcher;
  
  protected NodeWatcherWrapper(NodeWatcher nodeWatcher) {
    this.nodeWatcher = nodeWatcher;
  }
  
  public NodeWatcher getNodeWatcher() { return nodeWatcher; }
}