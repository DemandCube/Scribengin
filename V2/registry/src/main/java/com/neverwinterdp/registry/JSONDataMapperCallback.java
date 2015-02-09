package com.neverwinterdp.registry;

import com.neverwinterdp.util.JSONSerializer;

public class JSONDataMapperCallback<T> implements DataMapperCallback<T> {
  final static public JSONDataMapperCallback<?> INSTANCE = new JSONDataMapperCallback<>();
  
  public JSONDataMapperCallback() {
  }
  
  @Override
  public T map(String path, byte[] data, Class<T> type) {
    return JSONSerializer.INSTANCE.fromBytes(data, type);
  }
}
