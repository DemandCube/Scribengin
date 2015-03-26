package com.neverwinterdp.kafka.tool;

import static org.junit.Assert.assertFalse;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.producer.DefaultKafkaWriter;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.kafka.tool.server.KafkaCluster;


public class KafkaToolUnitTest {

  private static String topic = "delete-me";
  private DefaultKafkaWriter writer;
  private Logger logger = Logger.getLogger(getClass());
  private KafkaCluster cluster;
  private int brokers = 2;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/kafka", 1, brokers);
    cluster.setReplication(brokers);
    cluster.setNumOfPartition(1);
    cluster.start();
    Thread.sleep(2000);
  }

  @After
  public void tearDown() throws Exception {
    logger.info("from here we shutdown");
    cluster.shutdown();
    Thread.sleep(2000);
  }

  @Test
  public void testDeleteTopic() throws Exception {
    KafkaTool tool = new KafkaTool(topic, cluster.getZKConnect());
    writer = new DefaultKafkaWriter(topic, cluster.getKafkaConnect());
    String message = "sample message";
    for (int i = 0; i < 1000; i++) {
      writer.send(topic, 0, message + i, message + i, 10000);
    }
    tool.deleteTopic(topic);
    Thread.sleep(brokers * 1000);
    assertFalse(tool.topicExits(topic));
    tool.close();
  }

  @Test
  public void testDeleteTopicWithData() throws Exception {
    KafkaTool tool = new KafkaTool(topic, cluster.getZKConnect());
    writer = new DefaultKafkaWriter(topic, cluster.getKafkaConnect());
    byte[] data = new byte[500 * 1024];
    String message = new String(data);
    for (int i = 0; i < 1000; i++) {
      writer.send(topic, 0, String.valueOf(i), message + i, 10000);
    }

    tool.deleteTopic(topic);
   
    Thread.sleep(brokers * 3000);
    assertFalse(tool.topicExits(topic));
    tool.close();
  }
}
