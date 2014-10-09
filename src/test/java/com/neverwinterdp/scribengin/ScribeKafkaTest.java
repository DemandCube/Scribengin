package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.neverwinterdp.scribengin.fixture.KafkaFixture;
import com.neverwinterdp.scribengin.fixture.ZookeeperFixture;
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumer;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;


public class ScribeKafkaTest {

  private static final Logger logger = Logger.getLogger(ScribeKafkaTest.class);
  private static ZookeeperFixture zookeeper;
  private static KafkaFixture kf1;
  private static KafkaFixture kf2;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    System.out.println("calling setup..."); //xxx
    Process p = Runtime.getRuntime().exec("script/bootstrap_kafka.sh servers");
    p.waitFor();
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(p.getInputStream()));

    String line = "";
    while ((line = reader.readLine()) != null) {
      System.out.println(line);//xxx
    }

    zookeeper = new ZookeeperFixture("0.8.1", "127.0.0.1", 2323);
    zookeeper.start();

    System.out.println("about to start kafka");//xxx
    kf1 = new KafkaFixture("0.8.1.1", "127.0.0.1", 19876,
        zookeeper.getHost(),
        zookeeper.getPort());
    kf2 = new KafkaFixture("0.8.1.1", "127.0.0.1", 19877,
        zookeeper.getHost(),
        zookeeper.getPort());

    kf1.start();
    kf2.start();
  }


  @AfterClass
  public static void teardown() throws IOException {
    System.out.println("calling teardown.."); //xxx
    kf1.stop();
    kf2.stop();
    zookeeper.stop();
  }


  @Test
  public void testGetEarliestOffsetFromKafka() {
    logger.info("testKafka. ");
    int expectedInitialOffset=0;
    String topic = "testData";
    injectTestData(topic);

    ScribeConsumerConfig config = new ScribeConsumerConfig(topic);
    config.cleanStart = true;
    config.partition = 0;
    config.topic = topic;

    config.brokerList = getKafkaCluster();
    ScribeConsumer sc = new ScribeConsumer(config);
    sc.connectToTopic();
    try {
      Method method =
          ScribeConsumer.class.getDeclaredMethod("getEarliestOffsetFromKafka", String.class,
              int.class, long.class);
      method.setAccessible(true);
      long offset = (Long) method.invoke(sc, topic, 0, kafka.api.OffsetRequest.EarliestTime());
      Assert.assertTrue(offset == expectedInitialOffset);
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      logger.error(e.getCause());
    }
  }

  private List<HostPort> getKafkaCluster() {
    List<HostPort> hosts = Lists.newArrayList();
    HostPort hostPort1 = new HostPort(kf1.getHost(), kf1.getPort());
    hosts.add(hostPort1);

    HostPort hostPort2 = new HostPort(kf2.getHost(), kf2.getPort());
    hosts.add(hostPort2);

    return hosts;
  }


  public void injectTestData(String topic) {
    logger.info("injectTestData. ");
    Properties props = new Properties();
    props.put("metadata.broker.list", getKafkaURLs());
    props.put("producer.type", "async");
    logger.info("Kafka URLs " + getKafkaURLs());
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);

    for (int messages = 0; messages < 100; messages++) {
      String msg = UUID.randomUUID().toString();
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(
          topic, msg);
      try {
        producer.send(data);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    producer.close();
  }


  private Object getKafkaURLs() {
    logger.info("getKafkaURLs. ");
    StringBuilder builder = new StringBuilder();

    builder.append(kf1.getHost());
    builder.append(":");
    builder.append(kf1.getPort());
    builder.append(",");

    builder.append(kf2.getHost());
    builder.append(":");
    builder.append(kf2.getPort());
    builder.append(",");

    return builder.substring(0, builder.length() - 1);
  }
}
