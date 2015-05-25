package com.neverwinterdp.registry.event;

import com.neverwinterdp.registry.Registry;

abstract public class NodeEventListener<T extends Event> extends NodeEventWatcher {

  public NodeEventListener(Registry registry, boolean persistent) {
    super(registry, persistent);
  }
  
  public void processNodeEvent(NodeEvent event) throws Exception {
    T appEvent = toAppEvent(getRegistry(), event) ;
    onEvent(appEvent);
  }
  
  
  abstract public T toAppEvent(Registry registry, NodeEvent nodeEvent) throws Exception ;

  abstract public void onEvent(T event) throws Exception ;
}