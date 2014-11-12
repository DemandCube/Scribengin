package com.neverwinterdp.scribengin.registry.zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.neverwinterdp.scribengin.registry.NodeEvent;
import com.neverwinterdp.scribengin.registry.NodeWatcher;

public class ZKNodeWatcher implements Watcher {
  private String basePath ;
  private NodeWatcher nodeWatcher;
  
  public ZKNodeWatcher(String basePath, NodeWatcher nodeWatcher) {
    this.basePath = basePath ;
    this.nodeWatcher = nodeWatcher;
  }
  
  public void process(WatchedEvent event) {
    NodeEvent nEvent = new NodeEvent(realPath(event.getPath()), eventType(event)) ;
    nodeWatcher.process(nEvent);
  }
  
  private String realPath(String path) {
    if(path.length() == basePath.length()) return "/" ;
    return path.substring(basePath.length()) ;
  }
  
  private NodeEvent.Type eventType(WatchedEvent event) {
    if(event.getType() == Watcher.Event.EventType.NodeDeleted) return NodeEvent.Type.DELETE ;
    return NodeEvent.Type.OTHER ;
  }
}