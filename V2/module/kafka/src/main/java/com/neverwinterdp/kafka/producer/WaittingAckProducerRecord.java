package com.neverwinterdp.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WaittingAckProducerRecord<K, V> {
  private long                 id;
  private ProducerRecord<K, V> producerRecord;
  private Callback             callback;
  private int                  retryCount;
  private boolean              needToResend;
  
  public WaittingAckProducerRecord(long id, ProducerRecord<K, V> producerRecord, Callback callback) {
    this.id = id;
    this.producerRecord = producerRecord;
    this.callback = callback;
  }
  
  public long getId() { return id; }
  public void setId(long id) { this.id = id; }
  
  public ProducerRecord<K, V> getProducerRecord() { return producerRecord; }
  public void setProducerRecord(ProducerRecord<K, V> producerRecord) { this.producerRecord = producerRecord; }

  public Callback getCallback() { return callback; }
  public void setCallback(Callback callback) { this.callback = callback; }

  public int getRetryCount() { return retryCount; }
  public void setRetryCount(int retryCount) { this.retryCount = retryCount; }

  public boolean isNeedToResend() { return needToResend; }
  public void setNeedToResend(boolean needToResend) { this.needToResend = needToResend; }
}