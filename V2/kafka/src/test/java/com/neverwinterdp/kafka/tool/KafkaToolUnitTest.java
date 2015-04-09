package com.neverwinterdp.kafka.tool;

import static org.junit.Assert.assertFalse;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.kafka.producer.DefaultKafkaWriter;
import com.neverwinterdp.kafka.tool.server.KafkaCluster;


public class KafkaToolUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }
  static private KafkaCluster cluster;

  @BeforeClass
  static public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/kafka", 1, 2);
    cluster.setNumOfPartition(1);
    cluster.start();
    Thread.sleep(2000);
  }

  @AfterClass
  static public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testDeleteTopic() throws Exception {
    String topic = "hello-" + UUID.randomUUID().toString();
    KafkaTool tool = new KafkaTool(topic, cluster.getZKConnect());
    DefaultKafkaWriter writer = new DefaultKafkaWriter(topic, cluster.getKafkaConnect());
    String message = "sample message";
    for (int i = 0; i < 1000; i++) {
      writer.send(topic, 0, message + i, message + i, 10000);
    }
    writer.close();
    tool.deleteTopic(topic);
    Thread.sleep(1000);
    assertFalse(tool.topicExits(topic));
    tool.close();
  }

  @Test
  public void testDeleteTopicWithData() throws Exception {
    String topic = "hello-" + UUID.randomUUID().toString();
    KafkaTool tool = new KafkaTool(topic, cluster.getZKConnect());
    DefaultKafkaWriter writer = new DefaultKafkaWriter(topic, cluster.getKafkaConnect());
    byte[] data = new byte[1024];
    String message = new String(data);
    for (int i = 0; i < 1000; i++) {
      writer.send(topic, 0, String.valueOf(i), message + i, 10000);
    }
    writer.close();
    tool.deleteTopic(topic);
    Thread.sleep(1000);
    assertFalse(tool.topicExits(topic));
    tool.close();
  }
}
