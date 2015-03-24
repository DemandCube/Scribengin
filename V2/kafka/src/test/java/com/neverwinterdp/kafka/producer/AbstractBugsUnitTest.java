package com.neverwinterdp.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;

import com.neverwinterdp.server.kafka.KafkaCluster;

public abstract class AbstractBugsUnitTest {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  protected static String NAME = "test";
  protected static int    NUM_OF_SENT_MESSAGES = 50000;

  protected Logger logger = Logger.getLogger(getClass());
  protected KafkaCluster cluster;

  protected int kafkaBrokers;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/kafka", 1, getKafkaBrokers());
    cluster.setReplication(2);
    cluster.setNumOfPartition(1);
    cluster.start();
    Thread.sleep(2000);
  }

  public abstract int getKafkaBrokers();

  @After
  public void tearDown() throws Exception {
    logger.info("from here we shutdown");
    cluster.shutdown();
    Thread.sleep(3000);
  }

  protected DefaultKafkaWriter createKafkaWriter() {
    Map<String, String> kafkaProps = new HashMap<String, String>();
    kafkaProps.put("message.send.max.retries", "5");
    kafkaProps.put("retry.backoff.ms", "100");
    kafkaProps.put("queue.buffering.max.ms", "1000");
    kafkaProps.put("queue.buffering.max.messages", "15000");
    //kafkaProps.put("request.required.acks", "-1");
    kafkaProps.put("topic.metadata.refresh.interval.ms", "-1"); //negative value will refresh on failure
    kafkaProps.put("batch.num.messages", "100");
    kafkaProps.put("producer.type", "sync");
    //new config:
    kafkaProps.put("acks", "all");
    DefaultKafkaWriter writer = new DefaultKafkaWriter(NAME, kafkaProps, cluster.getKafkaConnect());
    logger.info("we have created a writer");
    return writer;
  }

  class MessageFailDebugCallback implements Callback {
    protected int failedCount;

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null)
        failedCount++;
    }
  }

}
