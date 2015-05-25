package com.neverwinterdp.buffer.chronicle;

public interface Serializer<T> {
  public byte[] toBytes(T object) ;
  public T fromBytes(byte[] data) ;
}
