package com.neverwinterdp.registry.event;

import com.neverwinterdp.registry.Registry;

abstract public class NodeChildrenListener<T extends Event> extends NodeChildrenWatcher {
  
  public NodeChildrenListener(Registry registry, boolean persistent) {
    super(registry, persistent);
  }
  
  @Override
  public void processNodeEvent(NodeEvent event) throws Exception {
    T appEvent = toAppEvent(getRegistry(), event) ;
    onEvent(appEvent);
  }
  
  abstract public T toAppEvent(Registry registry, NodeEvent nodeEvent) throws Exception ;

  abstract public void onEvent(T event) throws Exception ;
}