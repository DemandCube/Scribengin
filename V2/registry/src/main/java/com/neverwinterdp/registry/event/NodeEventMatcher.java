package com.neverwinterdp.registry.event;

import com.neverwinterdp.registry.Node;


public interface NodeEventMatcher {
  public boolean matches(Node node, NodeEvent event) throws Exception ;
}
