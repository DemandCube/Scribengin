package com.neverwinterdp.registry.event;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

abstract public class NodeChildrenWatcher extends NodeWatcher {
  private Registry registry;
  private boolean persistent ;
  private String  watchedPath = null ;
  
  public NodeChildrenWatcher(Registry registry, boolean persistent) {
    this.registry   = registry;
    this.persistent = persistent;
  }
  
  public Registry getRegistry() { return this.registry; }
  
  public void watchChildren(String path) throws RegistryException {
    if(watchedPath != null) {
      throw new RegistryException(ErrorCode.Unknown, "Already watched " + watchedPath) ;
    }
    registry.watchChildren(path, this);
  }
  
  @Override
  public void onEvent(NodeEvent event) {
    try {
      processNodeEvent(event);
    } catch(Exception ex) {
      ex.printStackTrace();
    }
    if(persistent) {
      try {
        if(isComplete()) return;
        registry.watchChildren(event.getPath(), this);
      } catch(RegistryException ex) {
        if(ex.getErrorCode() != ErrorCode.NoNode) {
          System.err.println("watch " + event.getPath() + ": " + ex.getMessage());
        } else {
          System.err.println("Stop watching " + event.getPath() + " due to the error: " + ex.getMessage());
        }
      }
    }
  }
  
  abstract public void processNodeEvent(NodeEvent event) throws Exception ;
}