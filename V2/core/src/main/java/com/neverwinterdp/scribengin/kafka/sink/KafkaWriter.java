package com.neverwinterdp.scribengin.kafka.sink;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import com.neverwinterdp.util.JSONSerializer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class KafkaWriter {
  private String name;
  private Properties kafkaProperties;
  private Producer<String, String> producer;
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
    kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
    kafkaProps.put("partitioner.class", SimplePartitioner.class.getName());
    kafkaProps.put("request.required.acks", "1");
    //TODO: Review, when the topic is auto created, producer can fail since kafka and zookeeper do not have enough time
    //to assign the partition leader for the first tries.
    kafkaProps.put("message.send.max.retries", "5");
    kafkaProps.put("topic.metadata.refresh.interval.ms", "0");
    kafkaProps.put("metadata.broker.list", kafkaBrokerUrls);
    if (props != null) {
      kafkaProps.putAll(props);
    }
    this.kafkaProperties = kafkaProps;
    reconnect();
  }

  public void reconnect() {
    if (producer != null) producer.close();
    ProducerConfig config = new ProducerConfig(kafkaProperties);
    producer = new Producer<String, String>(config);
  }

  public void send(String topic, String data) throws Exception {
    String key = name + idTracker.getAndIncrement();
    producer.send(new KeyedMessage<String, String>(topic, key, data));
  }
  
  public <T> void send(String topic, T obj) throws Exception {
    String json = JSONSerializer.INSTANCE.toString(obj);
    send(topic, json);
  }

  public void send(String topic, List<String> dataHolder) throws Exception {
    List<KeyedMessage<String, String>> holder = new ArrayList<KeyedMessage<String, String>>();
    for (int i = 0; i < dataHolder.size(); i++) {
      String key = name + idTracker.getAndIncrement();
      holder.add(new KeyedMessage<String, String>(topic, key, dataHolder.get(i)));
    }
    producer.send(holder);
  }

  public void close() {
    producer.close();
  }
}
