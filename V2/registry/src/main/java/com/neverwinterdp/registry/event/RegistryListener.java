package com.neverwinterdp.registry.event;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class RegistryListener {
  private Registry registry;
  private TreeMap<String, NodeWatcherWrapper> watchers = new TreeMap<String, NodeWatcherWrapper>() ;
  private boolean closed = false;
  
  public RegistryListener(Registry registry) {
    this.registry = registry;
  }

  public Registry getRegistry() { return this.registry ; }
  
  public TreeMap<String, NodeWatcherWrapper> getWatchers() { return this.watchers; }
  
  public void clear() {
    for(NodeWatcherWrapper sel : watchers.values()) {
      sel.setComplete();
    }
    watchers.clear();
  }
  
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
      wrapper = new PersistentModifyNodeWatcher(key, nodeWatcher);
    }
    registry.watchExists(path, wrapper);
    watchers.put(key, wrapper) ;
  }
  
  public void watch(String path, NodeWatcher nodeWatcher) throws RegistryException {
    watch(path, nodeWatcher, true) ;
  }
  
  public void watchModify(String path, NodeWatcher nodeWatcher, boolean persistent) throws RegistryException {
    String key = createKey(path, nodeWatcher);
    NodeWatcherWrapper wrapper = null ;
    if(!persistent) {
      wrapper = new OneTimeNodeWatcher(key, nodeWatcher);
    } else {
      wrapper = new PersistentModifyNodeWatcher(key, nodeWatcher);
    }
    registry.watchModify(path, wrapper);
    watchers.put(key, wrapper) ;
  }
  
  public void watchChildren(final String path, final NodeWatcher nodeWatcher, final boolean persistent, boolean waitIfNotExist) throws RegistryException {
    if(registry.exists(path)) {
      watchChildren(path, nodeWatcher, persistent);
    } else {
      NodeWatcher createWatcher = new NodeWatcher() {
        @Override
        public void onEvent(NodeEvent event) throws Exception {
          if(event.getType() == NodeEvent.Type.CREATE) {
            watchChildren(path, nodeWatcher, persistent);
          }
        }
      };
      watch(path, createWatcher, false) ;
      return;
    }
  }
  
  public void watchChildren(String path, NodeWatcher nodeWatcher, boolean persistent) throws RegistryException {
    String key = createKey(path, nodeWatcher);
    NodeWatcherWrapper wrapper = null ;
    if(!persistent) {
      wrapper = new OneTimeNodeWatcher(key, nodeWatcher);
    } else {
      wrapper = new PersistentChildrenNodeWatcher(key, nodeWatcher);
    }
    registry.watchChildren(path, wrapper);
    watchers.put(key, wrapper) ;
  }
  
  public void watchChild(String path, String childNameExp, NodeWatcher nodeWatcher) throws RegistryException {
    SelectChildNodeWatcher selectChildNodeWatcher = new SelectChildNodeWatcher(path, childNameExp, nodeWatcher) ;
    watchChildren(path, selectChildNodeWatcher, true, true);
  }
  
  public void watchHeartbeat(String path, NodeWatcher nodeWatcher) throws RegistryException {
    watch(path + "/heartbeat", nodeWatcher, true) ;
  }
  
  public void watchHeartbeat(Node node, NodeWatcher nodeWatcher) throws RegistryException {
    watch(node.getPath() + "/heartbeat", nodeWatcher, true) ;
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
  
  abstract class PersistentNodeWatcher extends NodeWatcherWrapper {
    String key ;
    
    PersistentNodeWatcher(String key, NodeWatcher nodeWatcher) { 
      super(nodeWatcher); 
      this.key = key;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      if(closed) return;
      try {
        if(isComplete() || nodeWatcher.isComplete()) {
          watchers.remove(key);
          return;
        }
        doWatch(event);
      } catch(RegistryException ex) {
        if(ex.getErrorCode() != ErrorCode.NoNode) {
          System.err.println("watch " + event.getPath() + ": " + ex.getMessage());
        } else {
          watchers.remove(key);
        }
      }
      nodeWatcher.onEvent(event);
      if(event.getType() == NodeEvent.Type.DELETE) {
        setComplete() ;
      }
    }
    
    abstract protected void doWatch(NodeEvent event) throws RegistryException ;
  }
  
  class PersistentModifyNodeWatcher extends PersistentNodeWatcher {
    PersistentModifyNodeWatcher(String key, NodeWatcher nodeWatcher) {
      super(key, nodeWatcher);
    }
    
    protected void doWatch(NodeEvent event) throws RegistryException {
      registry.watchModify(event.getPath(), this);
    }
  }
  
  class PersistentChildrenNodeWatcher extends PersistentNodeWatcher {
    PersistentChildrenNodeWatcher(String key, NodeWatcher nodeWatcher) {
      super(key, nodeWatcher);
    }
    
    protected void doWatch(NodeEvent event) throws RegistryException {
      registry.watchChildren(event.getPath(), this);
    }
  }
  
  class SelectChildNodeWatcher extends NodeWatcher {
    private String path = null;
    private Pattern selectChildPattern = null;
    private NodeWatcher watcher ;
    private Set<String> watchedChildren = new HashSet<String>() ;
    
    public SelectChildNodeWatcher(String path, String selectChildExp, NodeWatcher watcher) {
      this.path = path ;
      this.selectChildPattern = Pattern.compile(selectChildExp);
      this.watcher = watcher;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      System.err.println("SelectChildNodeWatcher " + event.getPath() + ", EVENT = " + event.getType());
      if(event.getType() == NodeEvent.Type.CHILDREN_CHANGED) {
        updateChildrenWatch();
      }
    }
    
    synchronized void updateChildrenWatch() throws Exception {
      List<String> childNames = null;
      try {
        childNames = registry.getChildren(path);
      } catch(RegistryException ex) {
        if(ex.getErrorCode() == ErrorCode.NoNode) {
          setComplete() ;
          return ;
        } else {
          throw ex ;
        }
      }
      System.err.println("UPDATE CHILDREN WATCH " + path);
      HashSet<String> childrenSet = new HashSet<String>() ;
      for(String childName : childNames) {
        childrenSet.add(childName) ;
        if(selectChildPattern.matcher(childName).matches()) {
          if(watchedChildren.contains(childName)) {
            continue;
          } else {
            //Detect new created child node
            String childPath = path + "/" + childName ;
            watchedChildren.add(childName);
            watchModify(childPath, watcher, true);
            watcher.onEvent(new NodeEvent(childPath, NodeEvent.Type.CREATE));
            System.err.println("DETECT NEW CHILD " + childPath + ", watcher = " + watcher);
          }
        }
      }
      Iterator<String> i = watchedChildren.iterator();
      while(i.hasNext()) {
        String childName = i.next();
        if(!childrenSet.contains(childName)) {
          i.remove();
        }
      }
    }
  }
  
  class OneTimeNodeWatcher extends NodeWatcherWrapper {
    private String      key;
    
    OneTimeNodeWatcher(String key, NodeWatcher nodeWatcher) {
      super(nodeWatcher);
      this.key = key;
    }
    
    @Override
    public void onEvent(NodeEvent event) throws Exception {
      if(closed) return;
      if(isComplete() || nodeWatcher.isComplete()) {
        watchers.remove(key);
        return;
      }
      nodeWatcher.onEvent(event);
      watchers.remove(key);
    }
  }
}