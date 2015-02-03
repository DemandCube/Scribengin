package com.neverwinterdp.registry.lock;

public interface LockOperation<T> {
  public T execute() throws Exception ;
}
