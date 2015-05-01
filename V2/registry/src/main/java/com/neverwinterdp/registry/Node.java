package com.neverwinterdp.registry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.event.NodeWatcher;
import com.neverwinterdp.registry.lock.Lock;

public class Node {
  private Registry  registry ;
  private String path  ;
  
  public Node(Registry registry, String path) {
    this.registry = registry ;
    this.path = path ;
  }

  public Registry getRegistry() { return this.registry ; }
  
  public String getPath() { return path; }

  public String getName() {
    int idx = path.lastIndexOf('/') ;
    return path.substring(idx + 1) ; 
  }
  
  public String getParentPath() {
    if(path.length() == 1 && path.equals("/")) return null ;
    int idx = path.lastIndexOf('/') ;
    return path.substring(0, idx) ; 
  }
  
  public Node getParentNode() { return new Node(registry, getParentPath()); }

  public boolean exists() throws RegistryException {
    return registry.exists(path) ;
  }
  
  public void create(NodeCreateMode mode) throws RegistryException {
    registry.create(path, mode);
  }
  
  public void create(byte[] data, NodeCreateMode mode) throws RegistryException {
    registry.create(path, data, mode);
  }
  
  public <T> void create(T data, NodeCreateMode mode) throws RegistryException {
    registry.create(path, data, mode);
  }
  
  public byte[] getData() throws RegistryException { 
    return registry.getData(path); 
  }
  
  public <T> T getDataAs(Class<T> type) throws RegistryException { 
    return registry.getDataAs(path, type);
  }

  public <T> T getDataAs(Class<T> type, DataMapperCallback<T> callback) throws RegistryException { 
    return registry.getDataAs(path, type, callback);
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
  
  public void rdelete() throws RegistryException {
    registry.rdelete(path);
  }
  
  public void rdelete(Transaction transaction) throws RegistryException {
    transaction.rdelete(path);
  }

  public Lock getLock(String name) { return new Lock(registry, path, name) ; }

  public LeaderElection getLeaderElection() { return new LeaderElection(registry, path) ; }

  public void watch(NodeWatcher watcher) throws RegistryException {
    registry.watchExists(path, watcher);
  }
  
  public void watchModify(NodeWatcher watcher) throws RegistryException {
    registry.watchModify(path, watcher);
  }
  
  public void watchChildren(NodeWatcher watcher) throws RegistryException {
    registry.watchChildren(path, watcher);
  }

  public void unwatch() throws RegistryException {
    throw new RegistryException(ErrorCode.Unknown, "Zookeeper does not support this method");
  }
  
  public boolean hasChild(String name) throws RegistryException {
    return registry.exists(path + "/" + name) ;
  }
  
  public Node getChild(String name) throws RegistryException {
    return new Node(registry, path + "/" + name) ;
  }
  
  public Node getDescendant(String descendant) throws RegistryException {
    return new Node(registry, path + "/" + descendant) ;
  }
  
  public List<String> getChildren() throws RegistryException {
    return registry.getChildren(path) ;
  }
  
  public List<String> getChildrenPath() throws RegistryException {
    return registry.getChildrenPath(path);
  }
  
  public <T> List<T> getChildrenAs(Class<T> type) throws RegistryException {
    return registry.getChildrenAs(path, type) ;
  }
  
  public <T> List<T> getSelectChildrenAs(List<String> names, Class<T> type) throws RegistryException {
    List<T> holder = new ArrayList<T>() ;
    for(int i = 0; i < names.size(); i++) {
      holder.add(registry.getDataAs(path + "/" + names.get(i), type)) ;
    }
    return holder ;
  }
  
  public <T> List<T> getChildrenAs(Class<T> type, boolean ignoreNoNodeError) throws RegistryException {
    return registry.getChildrenAs(path, type, ignoreNoNodeError) ;
  }
  
  public <T> List<T> getChildrenAs(Class<T> type, DataMapperCallback<T> callback) throws RegistryException {
    return registry.getChildrenAs(path, type, callback) ;
  }
  
  public <T> List<T> getChildrenAs(Class<T> type, DataMapperCallback<T> callback, boolean ignoreNoNodeError) throws RegistryException {
    return registry.getChildrenAs(path, type, callback, ignoreNoNodeError) ;
  }
  
  public Node createChild(String name, NodeCreateMode mode) throws RegistryException {
    return registry.create(path + "/" + name, mode);
  }

  public Node createChild(String name, byte[] data, NodeCreateMode mode) throws RegistryException {
    return registry.create(path + "/" + name, data, mode);
  }
  
  public <T> Node createChild(String name, T data, NodeCreateMode mode) throws RegistryException {
    return registry.create(path + "/" + name, data, mode);
  }
  
  public void createChildRef(String name, String toPath, NodeCreateMode mode) throws RegistryException {
    registry.createRef(path + "/" + name, toPath, mode);
  }
  
  public Node createDescendantIfNotExists(String descendant) throws RegistryException {
    registry.createIfNotExist(path + "/" + descendant) ;
    return new Node(registry, path + "/" + descendant);
  }
  
  public void dump(Appendable out) throws RegistryException, IOException  {
    List<String> childNodes = registry.getChildren(path);
    Collections.sort(childNodes);
    for(String node : childNodes) {
      dump(out, path, node, registry, "");
    }
  }
  
  public void dump(Appendable out, String indentation) throws RegistryException, IOException  {
    try {
      out.append(indentation).append(path).append("\n");
      List<String> childNodes = registry.getChildren(path);
      Collections.sort(childNodes);
      for(String node : childNodes) {
        dump(out, path, node, registry, indentation + "  ");
      }
    } catch(RegistryException ex) {
    }
  }
  
  private void dump(Appendable out, String parent, String node, Registry registry, String indentation) throws IOException, RegistryException {
    //During the recursive traverse, a node can be added or removed by the other process
    //So we can ignore all the No node exists exception
    String path = parent + "/" + node;
    if("/".equals(parent)) path = "/" + node;
    byte[] data = {};
    try {
      data = registry.getData(path);
    } catch(RegistryException ex) {
    }
    String stringData = "";
    if(data != null && data.length > 0) {
      stringData = " - " + new String(data);
      stringData = stringData.replace("\r\n", " ");
      stringData = stringData.replace("\n", " ");
      if(stringData.length() > 80) {
        stringData = stringData.substring(0, 80);
      }
    }
    out.append(indentation + node + stringData).append('\n');
    List<String > children = null ;
    try {
      children = registry.getChildren(path);
    } catch(RegistryException ex) {
    }
    if(children != null) {
      Collections.sort(children);
      for(String child : children) {
        dump(out, path, child, registry, indentation + "  ");
      }
    }
  }
}
