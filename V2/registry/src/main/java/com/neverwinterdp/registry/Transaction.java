package com.neverwinterdp.registry;


public interface Transaction {
  public Transaction create(final String path, byte data[], NodeCreateMode mode);

  public Transaction delete(final String path) ;
  
  public void rcopy(String path, String toPath) throws RegistryException ;
  public Transaction rdelete(final String path) throws RegistryException ;

  public Transaction check(String path) ;

  public Transaction setData(final String path, byte data[]) ;
  
  public <T> Transaction setData(final String path, T obj) ;
  
  public <T> Transaction setData(Node node, byte[] data) ;
  
  public <T> Transaction setData(Node node, T obj) ;

  public Transaction createChild(Node node, String name, NodeCreateMode mode);
  
  public Transaction createChild(Node node, String name, byte data[], NodeCreateMode mode);
  
  public <T> Transaction createChild(Node node, String name, T obj, NodeCreateMode mode);
  
  public Transaction createDescendant(Node node, String relativePath, NodeCreateMode mode);
  
  public Transaction createDescendant(Node node, String relativePath, byte data[], NodeCreateMode mode);
  
  public <T> Transaction createDescendant(Node node, String relativePath, T obj , NodeCreateMode mode);
 
  public Transaction deleteChild(Node node, String name);
  
  public Transaction deleteDescendant(Node node, String relativePath) ;
  
  public void commit() throws RegistryException ;
}
