package com.neverwinterdp.kafka.producer;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;

import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
abstract public class AbstractKafkaWriter implements KafkaWriter {
  final static public  Charset UTF8 = Charset.forName("UTF-8") ;
  
  private String name;
  private AtomicLong idTracker = new AtomicLong();

  public String getName() { return name; }

  public void setName(String name) { this.name = name; }

  public AbstractKafkaWriter(String name) {
    this.name = name;
  }

  @Override
  public void send(String topic, String data, long timeout) throws Exception {
    send(topic, nextKey(-1), data, timeout);
  }

  @Override
  public void send(String topic, String key, String data, long timeout) throws Exception {
    send(topic, -1, key, data, null, timeout);
  }

  @Override
  public void send(String topic, int partition, String key, String data, long timeout) throws Exception {
    send(topic, partition, key, data, null, timeout);
  }

  @Override
  public <T> void send(String topic, T obj, long timeout) throws Exception {
    send(topic, -1, nextKey(-1), JSONSerializer.INSTANCE.toString(obj), null, timeout);
  }

  public void send(String topic, int partition, String key, String data, Callback callback, long timeout) throws Exception {
    byte[] keyBytes = key.getBytes(UTF8);
    byte[] messageBytes = data.getBytes(UTF8);
    send(topic, partition, keyBytes, messageBytes, callback, timeout);
  }
  
  private String nextKey(int partition) {
    if(partition >= 0) {
      return "p:" + partition + ":" + idTracker.incrementAndGet();
    } else {
      return Long.toString(idTracker.incrementAndGet());
    }
  }
}