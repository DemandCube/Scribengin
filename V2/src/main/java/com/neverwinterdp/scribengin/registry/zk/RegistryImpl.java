package com.neverwinterdp.scribengin.registry.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;

import com.neverwinterdp.scribengin.registry.ErrorCode;
import com.neverwinterdp.scribengin.registry.Node;
import com.neverwinterdp.scribengin.registry.NodeCreateMode;
import com.neverwinterdp.scribengin.registry.NodeWatcher;
import com.neverwinterdp.scribengin.registry.Registry;
import com.neverwinterdp.scribengin.registry.RegistryConfig;
import com.neverwinterdp.scribengin.registry.RegistryException;
import com.neverwinterdp.util.JSONSerializer;

public class RegistryImpl implements Registry {
  public final Id ANYONE_ID = new Id("world", "anyone");
  public final ArrayList<ACL> DEFAULT_ACL = new ArrayList<ACL>(Collections.singletonList(new ACL(Perms.ALL, ANYONE_ID)));
  
  private String    basePath ;
  private String    zkConnect ;
  private ZooKeeper zkClient ;
  
  public RegistryImpl(String zkConnect, String basePath) {
    this.zkConnect = zkConnect ;
    this.basePath = basePath ;
  }
  
  public RegistryImpl(RegistryConfig config) {
    this.zkConnect = config.getConnect() ;
    this.basePath = config.getDbLocation() ;
  }
  
  public ZooKeeper getZkClient() { return this.zkClient ; }
  
  public String getSessionId() {
    if(zkClient == null) return null ;
    return Long.toString(zkClient.getSessionId()) ;
  }
  
  public String getBasePath() { return this.basePath ; }
  
  public Registry connect() throws RegistryException {
    try {
      zkClient = new ZooKeeper(zkConnect, 15000, new RegistryWatcher());
    } catch (IOException ex) {
      throw new RegistryException(ErrorCode.Connection, ex) ;
    }
    zkCreateIfNotExist(basePath) ;
    return this ;
  }
  
  public void disconnect() throws RegistryException {
    if(zkClient != null) {
      try {
        zkClient.close();
        zkClient = null ;
      } catch (InterruptedException e) {
        throw new RegistryException(ErrorCode.Connection, e) ;
      }
    }
  }
  
  @Override
  public Node create(String path, NodeCreateMode mode) throws RegistryException {
    return create(path, new byte[0], mode);
  }
  
  @Override
  public  Node create(String path, byte[] data, NodeCreateMode mode) throws RegistryException {
    try {
      String retPath = zkClient.create(realPath(path), data, DEFAULT_ACL, toCreateMode(mode)) ;
      if(mode == NodeCreateMode.PERSISTENT_SEQUENTIAL || mode == NodeCreateMode.EPHEMERAL_SEQUENTIAL) {
        path = retPath.substring(basePath.length()) ;
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
  
  
  public List<String> getChildren(String dir) throws RegistryException {
    try {
      List<String> names = zkClient.getChildren(realPath(dir), false);
      return names ;
    } catch (KeeperException | InterruptedException e) {
      throw new RegistryException(ErrorCode.Unknown, e) ;
    }
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
      Stat stat = zkClient.exists(realPath(path), new ZKNodeWatcher(basePath, watcher)) ;
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
    if(path.equals("/")) return basePath ;
    return basePath + path; 
  }
}
