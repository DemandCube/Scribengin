package com.neverwinterdp.registry;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class RegistryListener {
  @Inject
  private Registry registry;
  private TreeMap<String, NodeWatcherWrapper> watchers = new TreeMap<String, NodeWatcherWrapper>() ;
  private boolean closed = false;
  
  public RegistryListener() { }
  
  public RegistryListener(Registry registry) {
    this.registry = registry;
  }

  public TreeMap<String, NodeWatcherWrapper> getWatchers() { return this.watchers; }
  
  public void watch(String path, NodeWatcher nodeWatcher, boolean persistent) throws RegistryException {
    if(registry.exists(path)) {
      watchModify(path, nodeWatcher, persistent);
      return;
    }
    
    String key = createKey(path, nodeWatcher);
    NodeWatcherWrapper wrapper = null ;
    if(!persistent) {
      wrapper = new OneTimeNodeWatcher(key, nodeWatcher);
    } else {
      wrapper = new PersistentNodeWatcher(key, nodeWatcher);
    }
    registry.watchExists(path, wrapper);
    watchers.put(key, wrapper) ;
  }
  
  public void watchModify(String path, NodeWatcher nodeWatcher, boolean persistent) throws RegistryException {
    String key = createKey(path, nodeWatcher);
    NodeWatcherWrapper wrapper = null ;
    if(!persistent) {
      wrapper = new OneTimeNodeWatcher(key, nodeWatcher);
    } else {
      wrapper = new PersistentNodeWatcher(key, nodeWatcher);
    }
    registry.watchModify(path, wrapper);
    watchers.put(key, wrapper) ;
  }
  
  public void close() { closed = true; }
  
  public void dump(Appendable out) throws IOException {
    for(Map.Entry<String, NodeWatcherWrapper> entry : watchers.entrySet()) {
      out.append(entry.getKey()).append("\n");
    }
  }
  
  private String createKey(String path, NodeWatcher watcher) throws RegistryException {
    String key =  path + "[" + watcher.getClass().getName() + "#" + watcher.hashCode() + "]";
    if(watchers.containsKey(key)) {
      throw new RegistryException(ErrorCode.Unknown, "Already watch " + path + " with the watcher " + watcher.getClass()) ;
    }
    return key;
  }
  
  class PersistentNodeWatcher extends NodeWatcherWrapper {
    String key ;
    PersistentNodeWatcher(String key, NodeWatcher nodeWatcher) { 
      super(nodeWatcher); 
      this.key = key;
    }
    
    @Override
    public void process(NodeEvent event) {
      if(closed) return;
      try {
        if(isComplete()) {
          watchers.remove(key);
          return;
        }
        registry.watchModify(event.getPath(), this);
      } catch(RegistryException ex) {
        if(ex.getErrorCode() != ErrorCode.NoNode) {
          System.err.println("watch " + event.getPath() + ": " + ex.getMessage());
        } else {
          watchers.remove(key);
        }
      }
      nodeWatcher.process(event);
    }
  }
  
  class OneTimeNodeWatcher extends NodeWatcherWrapper {
    private String      key;
    
    OneTimeNodeWatcher(String key, NodeWatcher nodeWatcher) {
      super(nodeWatcher);
      this.key = key;
    }
    
    @Override
    public void process(NodeEvent event) {
      if(closed) return;
      nodeWatcher.process(event);
      watchers.remove(key);
    }
  }
}