package com.neverwinterdp.scribengin.registry;

import com.neverwinterdp.scribengin.registry.election.LeaderElection;
import com.neverwinterdp.scribengin.registry.lock.Lock;
import com.neverwinterdp.util.JSONSerializer;

public class Node {
  private RegistryService  registry ;
  private String path  ;
  
  public Node(RegistryService registry, String path) {
    this.registry = registry ;
    this.path = path ;
  }

  public RegistryService getRegistry() { return this.registry ; }
  
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
  
  public <T> T getData(Class<T> type) throws RegistryException { 
    byte[] data = getData();
    return JSONSerializer.INSTANCE.fromBytes(data, type);
  }

  public void setData(byte[] data) throws RegistryException {
    registry.setData(path, data);
  }
  
  public <T> void setData(T data) throws RegistryException {
    registry.setData(path, data);
  }

  public void delete() throws RegistryException {
    registry.delete(path);
  }

  public Lock getLock(String name) { return new Lock(registry, path, name) ; }

  public LeaderElection getLeaderElection() { return new LeaderElection(registry, path) ; }

  public void watch(NodeWatcher watcher) throws RegistryException {
    registry.watch(path, watcher);
  }

  public void unwatch() throws RegistryException {
  }
}
