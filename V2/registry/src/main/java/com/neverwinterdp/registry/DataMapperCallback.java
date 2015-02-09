package com.neverwinterdp.registry;

public interface DataMapperCallback<T> {
  public T map(String path, byte[] data, Class<T> type);
}
