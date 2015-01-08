package com.neverwinterdp.registry.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.ErrorCode;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.NodeCreateMode;
import com.neverwinterdp.registry.NodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;

@Singleton
public class RegistryImpl implements Registry {
  public final Id ANYONE_ID = new Id("world", "anyone");
  public final ArrayList<ACL> DEFAULT_ACL = new ArrayList<ACL>(Collections.singletonList(new ACL(Perms.ALL, ANYONE_ID)));
  
  @Inject
  private RegistryConfig config;
  private ZooKeeper zkClient ;
  
  public RegistryImpl() {
  }
  
  public RegistryImpl(RegistryConfig config) {
    this.config = config;
  }
  
  public RegistryConfig getRegistryConfig() {
    return this.config ;
  }
  
  public ZooKeeper getZkClient() { return this.zkClient ; }
  
  public String getSessionId() {
    if(zkClient == null) return null ;
    return Long.toString(zkClient.getSessionId()) ;
  }
  
  @Inject
  public void init() throws RegistryException {
    connect();
  }
  
  @Override
  public Registry connect() throws RegistryException {
    try {
      zkClient = new ZooKeeper(config.getConnect(), 15000, new RegistryWatcher());
    } catch (IOException ex) {
      throw new RegistryException(ErrorCode.Connection, ex) ;
    }
    zkCreateIfNotExist(config.getDbDomain()) ;
    return this ;
  }

  @Override
  public void disconnect() throws RegistryException {
    if(zkClient != null) {
      try {
        zkClient.close();;
        zkClient = null ;
      } catch (InterruptedException e) {
        throw new RegistryException(ErrorCode.Connection, e) ;
      }
    }
  }

  @Override
  public boolean isConnect() { return zkClient != null; }
  
  @Override
  public Node create(String path, NodeCreateMode mode) throws RegistryException {
    return create(path, new byte[0], mode);
  }
  
  @Override
  public  Node create(String path, byte[] data, NodeCreateMode mode) throws RegistryException {
    try {
      String retPath = zkClient.create(realPath(path), data, DEFAULT_ACL, toCreateMode(mode)) ;
      if(mode == NodeCreateMode.PERSISTENT_SEQUENTIAL || mode == NodeCreateMode.EPHEMERAL_SEQUENTIAL) {
        path = retPath.substring(config.getDbDomain().length()) ;
      }
      return new Node(this, path);
    } catch (KeeperException | InterruptedException e) {
      throw new RegistryException(ErrorCode.NodeCreation, e) ;
    }
  }
  
  @Override
  public <T> Node create(String path, T data, NodeCreateMode mode) throws RegistryException {
    byte[] bytes = JSONSerializer.INSTANCE.toBytes(data); 
    return create(path, bytes, mode);
  }
  
  @Override
  public Node createIfNotExist(String path) throws RegistryException {
    zkCreateIfNotExist(realPath(path));
    return new Node(this, path) ;
  }
  
  @Override
  public Node get(String path) throws RegistryException {
    return new Node(this, path) ;
  }
  
  @Override
  public byte[] getData(String path) throws RegistryException {
    try {
      return zkClient.getData(realPath(path), null, new Stat()) ;
    } catch (KeeperException | InterruptedException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
  
  @Override
  public <T> T getDataAs(String path, Class<T> type) throws RegistryException {
    try {
      byte[] bytes =  zkClient.getData(realPath(path), null, new Stat()) ;
      if(bytes == null || bytes.length == 0) return null;
      return JSONSerializer.INSTANCE.fromBytes(bytes, type);
    } catch (KeeperException | InterruptedException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
  
  public void setData(String path, byte[] data) throws RegistryException {
    try {
      Stat stat = zkClient.setData(realPath(path), data, -1) ;
    } catch (KeeperException | InterruptedException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
  
  public <T> void setData(String path, T data) throws RegistryException {
    byte[] bytes = JSONSerializer.INSTANCE.toBytes(data) ;
    setData(path, bytes);
  }
  
  
  public List<String> getChildren(String path) throws RegistryException {
    try {
      List<String> names = zkClient.getChildren(realPath(path), false);
      return names ;
    } catch (KeeperException | InterruptedException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
  
  public List<String> getChildren(String path, boolean watch) throws RegistryException {
    try {
      List<String> names = zkClient.getChildren(realPath(path), watch);
      return names ;
    } catch (KeeperException | InterruptedException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
  
  public <T> List<T> getChildrenAs(String path, Class<T> type) throws RegistryException {
    List<T> holder = new ArrayList<T>();
    List<String> nodes = getChildren(path);
    Collections.sort(nodes);
    for(int i = 0; i < nodes.size(); i++) {
      String name = nodes.get(i) ;
      Node node = get(path + "/" + name) ;
      T object = node.getData(type);
      holder.add(object);
    }
    return holder ;
  }

  @Override
  public boolean exists(String path) throws RegistryException {
    try {
      Stat stat = zkClient.exists(realPath(path), false) ;
      if(stat != null) return true ;
      return false ;
    } catch (KeeperException | InterruptedException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
  
  @Override
  public void watch(String path, NodeWatcher watcher) throws RegistryException {
    try {
      Stat stat = zkClient.exists(realPath(path), new ZKNodeWatcher(config.getDbDomain(), watcher)) ;
    } catch (KeeperException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    } catch (InterruptedException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
  
  @Override
  public void watchChildren(String path, NodeWatcher watcher) throws RegistryException {
    try {
      List<String> names = zkClient.getChildren(realPath(path), new ZKNodeWatcher(config.getDbDomain(), watcher)) ;
    } catch (KeeperException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    } catch (InterruptedException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
  
  @Override
  public void delete(String path) throws RegistryException {
    try {
      zkClient.delete(realPath(path), -1);
    } catch (InterruptedException | KeeperException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
 
  @Override
  public void rdelete(String path) throws RegistryException {
    try {
      PathUtils.validatePath(path);
      List<String> tree = ZKUtil.listSubTreeBFS(zkClient, realPath(path));
      for (int i = tree.size() - 1; i >= 0 ; --i) {
        //Delete the leaves first and eventually get rid of the root
        zkClient.delete(tree.get(i), -1); //Delete all versions of the node with -1.
      }
    } catch (InterruptedException | KeeperException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
  }
 
  
  private void zkCreateIfNotExist(String path) throws RegistryException {
    try {
      if (zkClient.exists(path, false) != null) new Node(this, path);
      StringBuilder pathB = new StringBuilder();
      String[] pathParts = path.split("/");
      for(String pathEle : pathParts) {
        if(pathEle.length() == 0) continue ; //root
        pathB.append("/").append(pathEle);
        String pathString = pathB.toString();
        //bother with the exists call or not?
        Stat nodeStat = zkClient.exists(pathString, false);
        if (nodeStat == null) {
          try {
            zkClient.create(pathString, null, DEFAULT_ACL, CreateMode.PERSISTENT);
          } catch(KeeperException.NodeExistsException ex) {
            break;
          }
        }
      }
    } catch(Exception ex) {
      throw new RegistryException(ErrorCode.NodeCreation, ex) ;
    }
  }
  
  private CreateMode toCreateMode(NodeCreateMode mode) {
    if(mode == NodeCreateMode.PERSISTENT) return CreateMode.PERSISTENT ;
    else if(mode == NodeCreateMode.PERSISTENT_SEQUENTIAL) return CreateMode.PERSISTENT_SEQUENTIAL ;
    else if(mode == NodeCreateMode.EPHEMERAL) return CreateMode.EPHEMERAL ;
    else if(mode == NodeCreateMode.EPHEMERAL_SEQUENTIAL) return CreateMode.EPHEMERAL_SEQUENTIAL ;
    throw new RuntimeException("Mode " + mode + " is not supported") ;
  }
  
  private String realPath(String path) { 
    if(path.equals("/")) return config.getDbDomain() ;
    return config.getDbDomain() + path; 
  }

  @Override
  public Registry newRegistry() throws RegistryException {
    return new RegistryImpl(config);
  }
}
