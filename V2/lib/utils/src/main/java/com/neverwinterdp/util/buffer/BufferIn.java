package com.neverwinterdp.util.buffer;

import java.util.List;

public interface BufferIn<T> {
  public void append(T obj, long timeout) ;
  public void append(T[] obj, long timeout) ;
  public void append(List<T> obj, long timeout) ;
}
