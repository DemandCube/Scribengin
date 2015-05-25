package com.neverwinterdp.registry;

public interface BatchOperations<T> {
  public T execute(Registry registry) throws RegistryException ;
}
