package com.neverwinterdp.kafka.producer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class WaittingAckProducerRecordHolder<K, V> {
  private int maxSize = 1000;
  private LinkedHashMap<Long, WaittingAckProducerRecord<K,V>> buffer = new LinkedHashMap<Long, WaittingAckProducerRecord<K, V>>();

  public int size() { return buffer.size(); }
  
  public List<WaittingAckProducerRecord<K,V>> getNeedToResendRecords() {
    List<WaittingAckProducerRecord<K,V>> holder = new ArrayList<WaittingAckProducerRecord<K,V>>();
    synchronized(buffer) {
      for(WaittingAckProducerRecord<K,V> sel : buffer.values()) {
        if(sel.isNeedToResend()) holder.add(sel);
      }
    }
    return holder;
  }
  
  public void add(WaittingAckProducerRecord<K, V> record, long timeout) throws Exception {
    if(buffer.size() >= maxSize) {
      waitForAvailableBuffer(timeout);
//      if(buffer.size() >= maxSize) {
//        long realWaitTime = System.currentTimeMillis() - startTime;
//        //throw new Exception("Cannot buffer the message after " + realWaitTime + "ms, message = " + record.getId()) ;
//      }
    }
    synchronized(buffer) {
      buffer.put(record.getId(), record);
    }
  }
  
  public WaittingAckProducerRecord<K,V> onSuccess(long recordId) {
    WaittingAckProducerRecord<K,V> record = null ;
    synchronized (buffer) {
      record = buffer.remove(recordId);
    }
    notifyForAvailableBuffer();
    return record;
  }
  
  public WaittingAckProducerRecord<K,V> onFail(long recordId) {
    WaittingAckProducerRecord<K,V> record = null ;
    synchronized(buffer) {
      record = buffer.get(recordId);
      record.setNeedToResend(true);
    }
    System.err.println("  fail on record " + recordId + ", buffer size = " + buffer.size());
    return record;
  }
  
  synchronized void waitForAvailableBuffer(long waitTime) throws InterruptedException {
    long start = System.currentTimeMillis();
    wait(waitTime);
    //System.err.println("  wait for max = " + waitTime + " before buffer, wait time = " + (System.currentTimeMillis() - start) +", buffer size = " + buffer.size());
  }
  
  synchronized void notifyForAvailableBuffer() {
    notify();
  }
}
