package com.neverwinterdp.scribengin.source.kafka;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;
import com.neverwinterdp.scribengin.fixture.KafkaFixture;
import com.neverwinterdp.scribengin.fixture.ZookeeperFixture;
import com.neverwinterdp.scribengin.source.SourceDescriptor;

public class KafkaSourceTest {

  private static final Logger logger = Logger.getLogger(KafkaSourceTest.class);
  static ZookeeperFixture zookeeperFixture;
  static Set<KafkaFixture> kafkaBrokers;
  private final static String topic = "scribe";
  private static KafkaSource source;
  private static int partitions = 2;
  private static SourceDescriptor descriptor;
  private static int kafkaPort = 9092;
  static final Random rnd = new Random();

  @BeforeClass
  public static void setup() throws Exception {
    BasicConfigurator.configure();
    kafkaBrokers = Sets.newLinkedHashSet();
    init();

    createKafkaData(100);
    descriptor = new SourceDescriptor();
    descriptor.setName(topic);
    descriptor.setLocation("127.0.0.1:9092");
    source = new KafkaSource(descriptor);
  }

  private static void init() throws IOException {
    zookeeperFixture = new ZookeeperFixture("0.8.1", "127.0.0.1", 2181);
    zookeeperFixture.start();

    KafkaFixture kafkaFixture;
    for (int i = 0; i < partitions; i++) {
      kafkaFixture = new KafkaFixture("0.8.1", "127.0.0.1", kafkaPort + i, "127.0.0.1", 2181);
      kafkaFixture.start();
      kafkaBrokers.add(kafkaFixture);
    }
  }

  @Test
  public void testGetSourceStreamInt() {
    assertEquals(1, source.getSourceStream(0));
  }

  @Test
  public void testGetSourceStreams() {
    assertEquals(partitions, source.getSourceStreams().length);
  }

  private static void createKafkaData(int startNum) {
    logger.info("createKafkaData. " + startNum);
    long events = Long.parseLong(startNum + "");
    Properties props = new Properties();
    props.put("metadata.broker.list", getBrokers());
    props.put("num.partitions", Integer.toString(partitions));
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("partitioner.class", "com.neverwinterdp.scribengin.source.kafka.SimplePartitioner");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);

    Producer<String, String> producer = new Producer<String, String>(config);

    for (long nEvents = 0; nEvents < events; nEvents++) {
      long runtime = new Date().getTime();
      String ip = "192.168.2." + rnd.nextInt(255);
      String msg = runtime + ",www.example.com," + ip;
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, ip, msg);
      producer.send(data);
    }
    producer.close();
  }

  private static String getBrokers() {
    StringBuilder builder = new StringBuilder();
    for (KafkaFixture kafkaFixture : kafkaBrokers) {
      builder.append(kafkaFixture.getHost());
      builder.append(':');
      builder.append(kafkaFixture.getPort());
      builder.append(',');
    }
    builder.setLength(builder.length() - 1);
    System.out.println("Bilder " + builder);
    return builder.toString();
  }



  @AfterClass
  public static void teardown() throws Exception {
    for (KafkaFixture kafkaFixture : kafkaBrokers) {
      kafkaFixture.stop();
    }
   // zookeeperFixture.stop();
  }
}
