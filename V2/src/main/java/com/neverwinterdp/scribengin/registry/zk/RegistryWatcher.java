package com.neverwinterdp.scribengin.registry.zk;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.neverwinterdp.scribengin.registry.NodeEvent;
import com.neverwinterdp.scribengin.registry.NodeWatcher;

public class RegistryWatcher implements Watcher {
  private Map<String, NodeWatcher> nodeWatchers = new HashMap<String, NodeWatcher>() ;
  
  public void add(String path, NodeWatcher watcher) {
  }
  
  public void process(WatchedEvent event) {
    if(event.getPath() != null) {
      NodeEvent nEvent = new NodeEvent(event.getPath(), NodeEvent.Type.OTHER) ;
      return ;
    }
    //System.out.println("on event: " + event.getPath() + " - " + event.getType() + " - " + event.getState());
  }
}