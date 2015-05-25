package com.neverwinterdp.buffer.chronicle;

import java.io.IOException;

import com.neverwinterdp.util.IOUtil;

public class JavaSerializer<T> implements Serializer<T> {
  public byte[] toBytes(T object) {
    try {
      return IOUtil.serialize(object);
    } catch (IOException e) {
      throw new RuntimeException(e) ;
    }
  }

  public T fromBytes(byte[] data) {
    try {
      return (T) IOUtil.deserialize(data);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
