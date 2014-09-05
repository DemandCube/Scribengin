package com.neverwinter.scribengin.impl;

import java.util.Properties;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;
import org.apache.log4j.Logger;

import com.neverwinter.scribengin.api.Reader;
import com.neverwinter.scribengin.api.ScribenginContext;


/*
 * 1. Get offset to read from from zookeeper
 * 2. Read from kafka broker
 * 3. If success record the offset in ZK
 * 4. If failure don't tell zk thus other reader can read offset
 * 5. allow for consumer groups
 */
// TODO metrics
public class KafkaReader implements Reader<ByteBufferMessageSet> {


  private static final Logger logger = Logger.getLogger(KafkaReader.class);
  private ScribenginContext scribenginContext;

  private Properties properties;
  int offset = 0;

  //put converter data
  //put offset we have handled
  @Override
  public boolean execute(Context context) throws Exception {
    scribenginContext = (ScribenginContext) context;
    scribenginContext.put(ScribenginContext.KAFKA_OFFSET, offset);
    // logger.info("Praperties " + properties);
    properties = scribenginContext.getProps();
    scribenginContext.put(ScribenginContext.CONVERTER_DATA, read());

    return Chain.CONTINUE_PROCESSING;
  }

  @Override
  public ByteBufferMessageSet read() {
    logger.info("read.");

    String topic = properties.getProperty("kafka.topic");
    int partition = Integer.parseInt(properties.getProperty("kafka.partition"));
    int kafkaPort = Integer.parseInt(properties.getProperty("kafka.port"));
    String kafkaHost = properties.getProperty("kafka.broker.list");
    String clientId = "tributary1";
    int soTimeout = 10000;
    int bufferSize = Integer.parseInt(properties.getProperty("kafka.buffer.size"));

    SimpleConsumer consumer =
        new SimpleConsumer(kafkaHost, kafkaPort, soTimeout, bufferSize, clientId);
    FetchRequest req = new FetchRequestBuilder()
        .clientId(clientId)
        .addFetch(topic, partition, offset, 100000)
        .build();

    FetchResponse resp = consumer.fetch(req);
    return resp.messageSet(topic, partition);
  }
}
