package com.neverwinterdp.scribengin.registry;

public interface Registry {
  public Registry connect() throws RegistryException ;
  public void disconnect() throws RegistryException ;
  
  public Node create(String path, NodeCreateMode mode) throws RegistryException ;
  public Node create(String path, byte[] data, NodeCreateMode mode) throws RegistryException ;
  public Node create(String path, byte[] data, NodeWatcher watcher, NodeCreateMode mode) throws RegistryException ;
  public <T> Node create(String path, T data, NodeCreateMode mode) throws RegistryException ;
  public <T> Node create(String path, T data, NodeWatcher watcher, NodeCreateMode mode) throws RegistryException ;
  public Node createIfNotExist(String path) throws RegistryException ;

  public Node get(String path) throws RegistryException ;
  public byte[] getData(String path) throws RegistryException ;
  
  public boolean exists(String path) throws RegistryException ;
  
  public void remove(String path) throws RegistryException ;
}
