package com.neverwinterdp.kafka.producer;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class DefaultKafkaWriter extends AbstractKafkaWriter {
  final static public  Charset UTF8 = Charset.forName("UTF-8") ;
  
  private Properties kafkaProperties;
  private KafkaProducer<byte[], byte[]> producer;

  public DefaultKafkaWriter(String name, String kafkaBrokerUrls) {
    this(name, null, kafkaBrokerUrls);
  }

  public DefaultKafkaWriter(String name, Map<String, String> props, String kafkaBrokerUrls) {
    super(name);
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
  
  public void close() { producer.close(); }
}