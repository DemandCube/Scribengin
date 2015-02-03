package com.neverwinterdp.scribengin.kafka.source;

import java.util.Iterator;
import java.util.List;

import com.neverwinterdp.util.JSONSerializer;

public class KafkaPartitionReaderIterator {
  private KafkaPartitionReader reader;
  private List<byte[]>         currentBuffer;
  private Iterator<byte[]>     currentBufferIterator;

  public KafkaPartitionReaderIterator(KafkaPartitionReader reader) {
    this.reader = reader;
  }
  
  public boolean hasNext() throws Exception {
    if(currentBufferIterator == null) {
      currentBuffer = reader.fetch(100000) ;
      currentBufferIterator = currentBuffer.iterator();
    }
    if(currentBufferIterator.hasNext()) return true;
    //fetch the next set
    currentBuffer = reader.fetch(100000) ;
    currentBufferIterator = currentBuffer.iterator();
    return currentBufferIterator.hasNext();
  }
  
  public byte[] next() {
    return currentBufferIterator.next();
  }
  
  public <T> T nextAs(Class<T> type) {
    byte[] data = currentBufferIterator.next();
    return JSONSerializer.INSTANCE.fromBytes(data, type);
  }
}
