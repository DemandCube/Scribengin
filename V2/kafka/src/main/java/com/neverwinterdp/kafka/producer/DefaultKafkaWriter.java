package com.neverwinterdp.kafka.producer;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class DefaultKafkaWriter implements KafkaWriter {
  final static public  Charset UTF8 = Charset.forName("UTF-8") ;
  
  private String name;
  private Properties kafkaProperties;
  private KafkaProducer<byte[], byte[]> producer;
  private AtomicLong idTracker = new AtomicLong();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DefaultKafkaWriter(String name, String kafkaBrokerUrls) {
    this(name, null, kafkaBrokerUrls);
  }

  public DefaultKafkaWriter(String name, Map<String, String> props, String kafkaBrokerUrls) {
    this.name = name;
    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", kafkaBrokerUrls);
    kafkaProps.put("value.serializer", ByteArraySerializer.class.getName());
    kafkaProps.put("key.serializer",   ByteArraySerializer.class.getName());
    this.kafkaProperties = kafkaProps;
    if (props != null) {
      kafkaProps.putAll(props);
    }
    this.kafkaProperties = kafkaProps;
    reconnect();
  }

  public void reconnect() {
    if (producer != null) producer.close();
    producer = new KafkaProducer<byte[], byte[]>(kafkaProperties);
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
  
  public void send(String topic, int partition, byte[] key, byte[] data, Callback callback, long timeout) throws Exception {
    ProducerRecord<byte[], byte[]> record = null;
    if(partition >= 0) record = new ProducerRecord<byte[], byte[]>(topic, partition, key, data);
    else record = new ProducerRecord<byte[], byte[]>(topic, key, data);
    producer.send(record, callback);
  }
  
  public void close() {
    producer.close();
  }
  
  private String nextKey(int partition) {
    if(partition >= 0) {
      return "p:" + partition + ":" + idTracker.incrementAndGet();
    } else {
      return Long.toString(idTracker.incrementAndGet());
    }
  }
}