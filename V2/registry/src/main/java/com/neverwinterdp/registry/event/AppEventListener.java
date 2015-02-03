package com.neverwinterdp.registry.event;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

abstract public class AppEventListener<T extends Event> extends NodeWatcher {
  private Registry registry;
  private boolean persistent ;
  private String  watchedPath = null ;
  
  public AppEventListener(Registry registry, boolean persistent) {
    this.registry   = registry;
    this.persistent = persistent;
  }
  
  public Registry getRegistry() { return this.registry; }
  
  public void watch(String path) throws RegistryException {
    if(registry.exists(path)) {
      watchModify(path);
      return;
    }
    watchExists(path);
  }
  
  public void watchModify(String path) throws RegistryException {
    if(watchedPath != null) {
      throw new RegistryException(ErrorCode.Unknown, "Already watched " + watchedPath) ;
    }
    registry.watchModify(path, this);
  }
  
  public void watchExists(String path) throws RegistryException {
    if(watchedPath != null) {
      throw new RegistryException(ErrorCode.Unknown, "Already watched " + watchedPath) ;
    }
    registry.watchExists(path, this);
  }
  
  @Override
  public void onEvent(NodeEvent event) {
    if(persistent) {
      try {
        if(isComplete()) return;
        registry.watchModify(event.getPath(), this);
      } catch(RegistryException ex) {
        if(ex.getErrorCode() != ErrorCode.NoNode) {
          System.err.println("watch " + event.getPath() + ": " + ex.getMessage());
        } else {
          System.err.println("Stop watching " + event.getPath() + " due to the error: " + ex.getMessage());
        }
      }
    }
    try {
      T appEvent = toAppEvent(registry, event) ;
      onEvent(appEvent);
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }
  
  abstract public T toAppEvent(Registry registry, NodeEvent nodeEvent) throws Exception ;

  abstract public void onEvent(T event) throws Exception ;
}