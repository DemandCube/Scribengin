package com.neverwinterdp.kafka.producer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class KafkaWriter {
  private String name;
  private Properties kafkaProperties;
  private KafkaProducer<String, String> producer;
  private AtomicLong idTracker = new AtomicLong();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public KafkaWriter(String name, String kafkaBrokerUrls) {
    this(name, null, kafkaBrokerUrls);
  }

  public KafkaWriter(String name, Map<String, String> props, String kafkaBrokerUrls) {
    this.name = name;
    Properties kafkaProps = new Properties();
    kafkaProps.put("bootstrap.servers", kafkaBrokerUrls);
    kafkaProps.put("value.serializer", StringSerializer.class.getName());
    kafkaProps.put("key.serializer",   StringSerializer.class.getName());
    if (props != null) {
      kafkaProps.putAll(props);
    }
    this.kafkaProperties = kafkaProps;
    reconnect();
  }

  public void reconnect() {
    if (producer != null) producer.close();
    producer = new KafkaProducer<String, String>(kafkaProperties);
  }

  public void send(String topic, String data, long timeout) throws Exception {
    String key = name + idTracker.getAndIncrement();
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, data);
    producer.send(record);
  }
  
  public void send(String topic, String key, String data, long timeout) throws Exception {
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, data);
    producer.send(record);
  }
  
  public void send(String topic, int partition, String key, String data, long timeout) throws Exception {
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition, key, data);
    producer.send(record);
  }
  
  public void send(String topic, int partition, String key, String data, Callback callback, long timeout) throws Exception {
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, partition, key, data);
    producer.send(record, callback);
  }
  
  public <T> void send(String topic, T obj, long timeout) throws Exception {
    String json = JSONSerializer.INSTANCE.toString(obj);
    send(topic, json, timeout);
  }

  public void send(String topic, List<String> dataHolder, long timeout) throws Exception {
    for (int i = 0; i < dataHolder.size(); i++) {
      String key = name + idTracker.getAndIncrement();
      ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, dataHolder.get(i));
      producer.send(record);
    }
  }

  public void close() {
    producer.close();
  }
}