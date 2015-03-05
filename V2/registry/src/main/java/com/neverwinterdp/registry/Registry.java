package com.neverwinterdp.registry;

import java.util.List;

import com.neverwinterdp.registry.event.NodeWatcher;

public interface Registry {
  public RegistryConfig getRegistryConfig() ;
  
  public Registry connect() throws RegistryException ;
  public Registry connect(long timeout) throws RegistryException;
  
  public void disconnect() throws RegistryException ;
  public boolean isConnect();
  
  public String getSessionId()  ;
  
  public Node create(String path, NodeCreateMode mode) throws RegistryException ;
  public void createRef(String path, String toPath, NodeCreateMode mode) throws RegistryException ;
  public Node create(String path, byte[] data, NodeCreateMode mode) throws RegistryException ;
  public <T> Node create(String path, T data, NodeCreateMode mode) throws RegistryException ;
  public Node createIfNotExist(String path) throws RegistryException ;

  public Node get(String path) throws RegistryException ;
  public Node getRef(String path) throws RegistryException ;
  public byte[] getData(String path) throws RegistryException ;
  public <T> T getDataAs(String path, Class<T> type) throws RegistryException ;
  public <T> T getDataAs(String path, Class<T> type, DataMapperCallback<T> mapper) throws RegistryException ;
  public <T> List<T> getDataAs(List<String> paths, Class<T> type) throws RegistryException;
  public <T> List<T> getDataAs(List<String> paths, Class<T> type, DataMapperCallback<T> mapper) throws RegistryException;
  public void setData(String path, byte[] data) throws RegistryException ;
  public <T> void setData(String path, T data) throws RegistryException ;
  
  public List<String> getChildren(String dir) throws RegistryException ;
  public List<String> getChildrenPath(String dir) throws RegistryException ;
  public List<String> getChildren(String path, boolean watch) throws RegistryException;
  public <T> List<T> getChildrenAs(String path, Class<T> type) throws RegistryException;
  public <T> List<T> getChildrenAs(String path, Class<T> type, DataMapperCallback<T> callback) throws RegistryException;
  
  public <T> List<T> getRefChildrenAs(String path, Class<T> type) throws RegistryException;
  
  public boolean exists(String path) throws RegistryException ;
  
  public void watchModify(String path, NodeWatcher watcher) throws RegistryException ;
  public void watchExists(String path, NodeWatcher watcher) throws RegistryException ;
  public void watchChildren(String path, NodeWatcher watcher) throws RegistryException;
  
  public void delete(String path) throws RegistryException ;
  public void rdelete(String path) throws RegistryException ;
  
  public void rcopy(String path, String toPath) throws RegistryException ;
  
  public Transaction getTransaction() ;
  
  public <T> T executeBatch(BatchOperations<T> ops, int retry, long timeoutThreshold) throws RegistryException ;
  
  /**
   * Create a new Registry session base on the current configuration
   */
  public Registry newRegistry() throws RegistryException;
}
