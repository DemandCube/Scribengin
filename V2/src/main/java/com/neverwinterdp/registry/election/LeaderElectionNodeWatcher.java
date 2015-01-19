package com.neverwinterdp.registry.election;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;

abstract public class LeaderElectionNodeWatcher<T> extends NodeWatcher {
  Registry registry;
  Class<T> descriptorType;
  
  public LeaderElectionNodeWatcher(Registry registry,Class<T> descriptorType) {
    this.registry = registry ;
    this.descriptorType = descriptorType;
  }
  
  @Override
  public void process(NodeEvent event) {
    try {
      String path = event.getPath();
      if(event.getType() == NodeEvent.Type.MODIFY) {
        Node node = registry.getRef(path);
        T data = node.getData(descriptorType) ;
        onElected(event, data);
      }
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  abstract public void onElected(NodeEvent event, T data) ;
}