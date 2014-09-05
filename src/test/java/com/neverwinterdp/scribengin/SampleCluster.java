package com.neverwinterdp.scribengin;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import org.apache.curator.test.TestingServer;

import com.google.common.base.Preconditions;

// TODO FutureTask instead of sleep
public class SampleCluster {

  private int kafkaListenPort;
  private String kafkaLogDir;
  private int zkListenPort;
  private int kafkaInstances;

  private String zookeeperConnectURL;
  private  TestingServer testingServer;
  private List<KafkaServer> kafkaServers;

  public List<KafkaServer> getKafkaServers() {
    return kafkaServers;
  }

  public void setKafkaServers(List<KafkaServer> kafkaServers) {
    this.kafkaServers = kafkaServers;
  }

  public SampleCluster(int zkListenPort, int kafkaInstances,
      int kafkaListenPort, String kafkaLogDir) {
    this.zkListenPort = zkListenPort;
    this.kafkaListenPort = kafkaListenPort;
    this.kafkaLogDir = kafkaLogDir;
    this.kafkaInstances = kafkaInstances;
    kafkaServers = new LinkedList<KafkaServer>();
  }

  // returns its connect URL
  public void startZookeeper() throws Exception {
    testingServer = new TestingServer(zkListenPort, true);
    testingServer.start();
    Thread.sleep(2000);
    zookeeperConnectURL = testingServer.getConnectString();
  }

  public List<KafkaServer> startKafkaInstances() {
    Preconditions.checkState(zookeeperConnectURL != null,
        "Zookeeper server has not yet been started.");
    Properties props;
    KafkaServer server;
    for (int i = 0; i < kafkaInstances; i++) {
      props = new Properties();
      props.setProperty("hostname", "127.0.0.1");
      props.setProperty("host.name", "127.0.0.1");
      props.setProperty("port", Integer.toString(kafkaListenPort + i));
      props.setProperty("broker.id", Integer.toString(i));
      props.setProperty("auto.create.topics.enable", "true");
      props.setProperty("log.dirs", kafkaLogDir + "/" + i);
      props.setProperty("enable.zookeeper", "true");
      props.setProperty("zookeeper.connect", zookeeperConnectURL);
      server = new KafkaServer(new KafkaConfig(props), new MockTime());
      server.startup();
      kafkaServers.add(server);
    }
    return kafkaServers;
  }

  public void stopZookeeper() throws IOException {
    testingServer.stop();
  }

  public void stopKafka() {
    for (KafkaServer kafkaServer : kafkaServers) {
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();
    }
  }

  static public class MockTime implements Time {

    public long milliseconds() {
      return System.currentTimeMillis();
    }

    public long nanoseconds() {
      return System.nanoTime();
    }

    public void sleep(long time) {
      try {
        Thread.sleep(time);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void start() {
    try {
      startZookeeper();
      startKafkaInstances();
      Thread.sleep(5000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void stop() {
    try {
      stopKafka();
      stopZookeeper();
      Thread.sleep(5000);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public String getzkURL() {
    return zookeeperConnectURL;
  }

  public void injectTestData(String topic) {

    Properties props = new Properties();
    props.put("metadata.broker.list", getKafkaURLs());
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);

    long events = 100;

    for (long nEvents = 0; nEvents < events; nEvents++) {
      String msg = UUID.randomUUID().toString();
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(
          topic, msg);
      producer.send(data);
    }
    producer.close();
  }

  private String getKafkaURLs() {
    StringBuilder builder = new StringBuilder();
    for (KafkaServer server : kafkaServers) {
      builder.append(server.config().advertisedHostName());
      builder.append(":");
      builder.append(server.config().advertisedPort());
      builder.append(",");
    }
    return builder.substring(0, builder.length() - 1);
  }
}
