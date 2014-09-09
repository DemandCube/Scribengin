package com.neverwinterdp.scribengin.readers;

import java.util.Properties;

import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.commons.chain.Chain;
import org.apache.commons.chain.Context;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.ScribenginContext;
import com.neverwinterdp.scribengin.reader.helpers.KafkaReaderSimpleConsumer;


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
  private KafkaReaderSimpleConsumer consumer;


  public KafkaReader() {
    consumer = new KafkaReaderSimpleConsumer();
  }

  private Properties properties;
 static int offset = 1;

  //get the offset we want
  //go read the data
  // put data in context
  //put offset we read in context.
  @Override
  public boolean execute(Context context) throws Exception {
    scribenginContext = (ScribenginContext) context;
    properties = scribenginContext.getProps();

    scribenginContext.put(ScribenginContext.CONVERTER_DATA, read());
    scribenginContext.put(ScribenginContext.KAFKA_OFFSET, offset);
    return Chain.CONTINUE_PROCESSING;
  }

  @Override
  public ByteBufferMessageSet read() {
    logger.info("read.");
    consumer.init(properties);

    return consumer.read(++offset);
  }
}
