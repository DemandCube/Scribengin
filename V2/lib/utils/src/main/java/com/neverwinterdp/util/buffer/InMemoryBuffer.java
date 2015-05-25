package com.neverwinterdp.util.buffer;

import java.util.LinkedList;
import java.util.List;

public class InMemoryBuffer<T> implements Buffer<T> {
  private LinkedList<InMemmoryBufferChunk<T>> chunks = new LinkedList<InMemmoryBufferChunk<T>>() ;
  
  public void append(T obj, long timeout) {
  }

  public void append(T[] obj, long timeout) {
  }

  public void append(List<T> obj, long timeout) {
  }

  public T next(long timeout) {
    return null;
  }

  public void next(List<T> holder, int max, long timeout) {
  }
}
