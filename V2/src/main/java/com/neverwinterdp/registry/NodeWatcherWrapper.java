package com.neverwinterdp.registry;

abstract public class NodeWatcherWrapper extends NodeWatcher {
  protected NodeWatcher nodeWatcher;
  
  NodeWatcherWrapper(NodeWatcher nodeWatcher) {
    this.nodeWatcher = nodeWatcher;
  }
  
  public NodeWatcher getNodeWatcher() { return nodeWatcher; }
}