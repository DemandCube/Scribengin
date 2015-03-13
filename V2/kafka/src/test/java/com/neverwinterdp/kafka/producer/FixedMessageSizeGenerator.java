package com.neverwinterdp.kafka.producer;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class FixedMessageSizeGenerator implements Iterator<String> {

  private static AtomicInteger sequenceID;
  private StringBuilder message;
  private int dataSize;

  public FixedMessageSizeGenerator(int dataSize) {
    this.dataSize = dataSize;
    sequenceID = new AtomicInteger();
    message = createMessage();
  }

  @Override
  public boolean hasNext() {
    return sequenceID.get() < Integer.MAX_VALUE;
  }

  @Override
  public String next() {
    String sequence = String.valueOf(sequenceID.getAndIncrement());
    message.replace(0, sequence.length(), sequence);
    return message.toString();
  }

  /**  * Creates a message of size dataSize in KB.  */
  private StringBuilder createMessage() {
    // Java chars are 2 bytes   
    dataSize = dataSize / 2;
    dataSize = dataSize * 1024;
    StringBuilder sb = new StringBuilder(dataSize);
    for (int i = 0; i < dataSize; i++) {
      sb.append('a');
    }
    return sb;
  }

  @Override
  public void remove() {
    sequenceID.decrementAndGet();
  }

  public int getCount() {
    return sequenceID.get();
  }
}
