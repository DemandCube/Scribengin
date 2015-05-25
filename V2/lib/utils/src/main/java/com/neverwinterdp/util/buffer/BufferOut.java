package com.neverwinterdp.util.buffer;

import java.util.List;

public interface BufferOut<T> {
  public T next(long timeout) ;
  public void next(List<T> holder, int max, long timeout) ;
}
