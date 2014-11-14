package com.neverwinterdp.scribengin.registry;

import com.neverwinterdp.scribengin.registry.lock.Lock;

public class Node {
  private Registry  registry ;
  private String path  ;
  
  public Node(Registry registry, String path) {
    this.registry = registry ;
    this.path = path ;
  }

  public Registry getRegistry() { return this.registry ; }
  
  public String getPath() { return path; }

  public String getParentPath() {
    if(path.length() == 1 && path.equals("/")) return null ;
    int idx = path.lastIndexOf('/') ;
    return path.substring(0, idx) ; 
  }

  public boolean exists() throws RegistryException {
    return registry.exists(path) ;
  }
  
  public byte[] getData() throws RegistryException { 
    return registry.getData(path); 
  }

  public void setData(byte[] data) throws RegistryException {
  }

  public void delete() throws RegistryException {
    registry.delete(path);
  }

  public Lock getLock(String name) throws RegistryException {
    return new Lock(registry, path, name) ;
  }

  public void watch(NodeWatcher watcher) throws RegistryException {
    registry.watch(path, watcher);
  }

  public void unwatch() throws RegistryException {
  }
}
