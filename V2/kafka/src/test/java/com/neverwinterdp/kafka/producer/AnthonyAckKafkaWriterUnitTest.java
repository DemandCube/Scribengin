package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;

/**
 * Unit tests for AckKafkaWriter
 */
public class AnthonyAckKafkaWriterUnitTest extends AbstractBugsUnitTest {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  private String topic = "test-topic";
  private AckKafkaWriter writer;

  @Override
  public int getKafkaBrokers() {
    return 2;
  }

  //This method tests the send(topic,partition, timeout) method
  // it throws IAE because of line 35 in com.neverwinterdp.kafka.producer.AbstractKafkaWriter sets the default partition to -1
  @Test
  public void testSendMethod() throws Exception {
    String message = "test-message-";
    int messageCount = 1000;

    writer = new AckKafkaWriter(topic, cluster.getKafkaConnect());
    for (int i = 0; i < messageCount; i++) {
      writer.send(topic, message + i, 10000);
    }
    assertEquals(messageCount, readMessages(messageCount));
  }

  //This method tests the send(topic, Object, timeout) method
  // it throws IAE because of line 45 in com.neverwinterdp.kafka.producer.AbstractKafkaWriter sets the default partition to -1
  @Test
  public void testSendObjectMethod() throws Exception {
    int messageCount = 1000;

    writer = new AckKafkaWriter(topic, cluster.getKafkaConnect());
    Person person;
    for (int i = 0; i < messageCount; i++) {
      person = new Person("anthony", i);
      writer.send(topic, person, 10000);
    }
    assertEquals(messageCount, readMessages(messageCount));
  }

  //This method tests the send(topic, key, message, timeout) method
  //it throws IAE because of line 35 in com.neverwinterdp.kafka.producer.AbstractKafkaWriter sets the default partition to -1
  @Test
  public void testSendKeyMethod() throws Exception {
    String message = "test-message-";
    int messageCount = 1000;

    writer = new AckKafkaWriter(topic, cluster.getKafkaConnect());
    for (int i = 0; i < messageCount; i++) {
      writer.send(topic, String.valueOf(i), message + i, 10000);
    }
    assertEquals(messageCount, readMessages(messageCount));
  }

  //TODO start topic with two partitions, 
  // assert that the send(String topic, int partition, String key, String data, long timeout) method writes 
  //only to specified partition
  @Test
  public void testSendKeyPartitionMethod() throws Exception {
    String message = "test-message-";
    int messageCount = 1000;

    writer = new AckKafkaWriter(topic, cluster.getKafkaConnect());
    for (int i = 0; i < messageCount; i++) {
      writer.send(topic, 0, String.valueOf(i), message + i, 10000);
    }
    assertEquals(messageCount, readMessages(messageCount));
  }

  //Test that the retry mechanism does not hinder succesful messages from being written
  //Every 10th message is too big thus ack writer will retry it. 
  @Test
  public void testRandomInvalidMessages() throws Exception {
    String message;
    int messageCount = 1000;
    int errors = 10;
    //an invalidly large message
    byte[] data = new byte[1024 * 1024];
    String bigMessage = new String(data);

    writer = new AckKafkaWriter(topic, cluster.getKafkaConnect());
    for (int i = 0; i < messageCount; i++) {
      message = "test-message-" + i;
      if (i % errors == 0) {
        message = bigMessage;
      }
      writer.send(topic, 0, String.valueOf(i), message, 10000);
    }

    int expected = messageCount - (messageCount / errors);
    System.out.println("Written " + expected);

    assertEquals(expected, readMessages(messageCount));
  }

  // Tests that sending messages of different sizes works fine.
  //max message size sendable by Ack writer with default config is 977*1024
  @Test
  public void testSendVariableMessageSize() throws Exception {
    String message = "message";
    int messageCount = 500;

    writer = new AckKafkaWriter(topic, cluster.getKafkaConnect());
    for (int i = 0; i < messageCount; i++) {

      message = getBigMessageOfSize(i);
      writer.send(topic, 0, String.valueOf(i), message, 10000);
    }
    assertEquals(messageCount, readMessages(messageCount));
  }

  private String getBigMessageOfSize(int i) {
    byte[] data = new byte[i * 1024];
    return new String(data);
  }

  private int readMessages(int expected) throws InterruptedException {
    String[] checkArgs = { "--topic", topic,
        "--consume-max", Integer.toString(expected),
        "--zk-connect", cluster.getZKConnect(),
        "--consume-batch-fetch", "10000"
    };
    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool(checkArgs);
    checkTool.runAsDeamon();
    if (checkTool.waitForTermination(10000)) {
      checkTool.setInterrupt(true);
      Thread.sleep(3000);
    }
    return checkTool.getMessageCounter().getTotal();
  }

  class Person {

    private String name;
    private int age;

    public Person(String name, int age) {
      super();
      this.name = name;
      this.age = age;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }
}