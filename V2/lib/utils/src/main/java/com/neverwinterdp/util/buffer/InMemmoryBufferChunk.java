package com.neverwinterdp.util.buffer;

import java.lang.instrument.Instrumentation;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InMemmoryBufferChunk<T> implements BufferChunk<T> {
  private static Instrumentation instrumentation;
  
  private BufferChunkHolder<T> holder  ;
  
  public InMemmoryBufferChunk(long maxSize) {
    holder = new BufferChunkHolder<T>(maxSize) ;
  }
  
  public long getMaxSize() { return 0; }

  public void append(T obj, long timeout) throws InterruptedException {
    holder.append(obj, timeout) ;
  }
  
  public T take(long timeout) throws InterruptedException {
    return holder.take(timeout) ;
  }
  
  static class Entry<T> {
    long size ;
    T    value ;
    
    Entry(T obj) {
      this.value = obj ;
      size = instrumentation.getObjectSize(obj);
    }
  }
  
  static class BufferChunkHolder<T> extends LinkedBlockingQueue<Entry<T>> {
    long maxSize ;
    long currentSize ;
    
    BufferChunkHolder(long maxSize) {
      this.maxSize = maxSize ;
    }
    
    public void append(T obj, long timeout) throws InterruptedException {
      Entry<T> entry = new Entry<T>(obj) ;
      if(offer(entry, timeout, TimeUnit.MILLISECONDS)) {
        currentSize += entry.size ;
      }
    }
    
    public T take(long timeout) throws InterruptedException {
      Entry<T> entry = poll(timeout, TimeUnit.MILLISECONDS) ;
      if(entry == null) return null ;
      currentSize -= entry.size ;
      return entry.value ;
    }
    
    public int remainingCapacity() {
      if(currentSize > maxSize) return 0 ;
      if(size() == 0) return (int) maxSize/1000 ;
      return (int)((maxSize - currentSize)/(long)size());
  }
  }
}
