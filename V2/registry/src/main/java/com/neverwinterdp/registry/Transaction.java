package com.neverwinterdp.registry;


public interface Transaction {
  public Transaction create(final String path, byte data[], NodeCreateMode mode);

  public Transaction delete(final String path) ;

  public Transaction check(String path) ;

  public Transaction setData(final String path, byte data[]) ;

  public void commit() throws RegistryException ;
}
