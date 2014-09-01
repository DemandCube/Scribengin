package com.neverwinterdp.scribengin;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import kafka.cluster.Broker;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

// TODO FutureTask instead of sleep
public class SampleKafkaCluster {

  private int kafkaListenPort;
  private String kafkaLogDir;
  private int zkListenPort;
  private int kafkaInstances;
  private boolean useExternalZk;
  private static TestingServer testingServer;
  private List<KafkaServer> kafkaServers;
  private List<Broker> kafkaServers2;

  private String zookeeperConnectURL;

  private static final Logger logger = Logger
      .getLogger(SampleKafkaCluster.class);

  public List<Broker> getKafkaBrokers() {
    return kafkaServers2;
  }

  public void setKafkaServers2(List<Broker> kafkaServers2) {
    this.kafkaServers2 = kafkaServers2;
  }

  public String getZookeeperConnectURL() {
    return zookeeperConnectURL;
  }

  public void setZookeeperConnectURL(String zookeeperConnectURL) {
    this.zookeeperConnectURL = zookeeperConnectURL;
  }

  public boolean isUseExternalZk() {
    return useExternalZk;
  }

  public void setUseExternalZk(boolean useExternalZk) {
    this.useExternalZk = useExternalZk;
  }


  public List<KafkaServer> getKafkaServers() {
    return kafkaServers;
  }

  public void setKafkaServers(List<KafkaServer> kafkaServers) {
    this.kafkaServers = kafkaServers;
  }

  public SampleKafkaCluster(int zkListenPort, int kafkaInstances,
      int kafkaListenPort, String kafkaLogDir) {
    this.zkListenPort = zkListenPort;
    this.kafkaListenPort = kafkaListenPort;
    this.kafkaLogDir = kafkaLogDir;
    this.kafkaInstances = kafkaInstances;
    kafkaServers = new LinkedList<KafkaServer>();
    kafkaServers2 = new LinkedList<Broker>();
  }

  // returns its connect URL
  public void startZookeeper() throws Exception {
    if (useExternalZk) {
      return;
    }
    testingServer = new TestingServer(zkListenPort, true);
    logger.debug("Testing server temp dir "
        + testingServer.getTempDirectory());
    testingServer.start();
    Thread.sleep(2000);
    zookeeperConnectURL = testingServer.getConnectString();
  }

  public List<KafkaServer> startKafkaInstances() {
    Preconditions.checkState(zookeeperConnectURL != null,
        "Zookeeper server has not yet been started.");
    logger.info("startKafkaInstances. " + kafkaInstances);
    File file = new File(kafkaLogDir);
    try {
      logger.info("Attempting to delete " + kafkaLogDir);
      FileUtils.deleteDirectory(file);
    } catch (IOException e) {
      e.printStackTrace();
    }
    logger.info("Exists " + file.exists());
    Properties props;
    KafkaServer server;
    Broker broker;
    for (int i = 0; i < kafkaInstances; i++) {
      props = new Properties();
      props.setProperty("host.name", "127.0.0.1");
      props.setProperty("port", Integer.toString(kafkaListenPort + i));
      props.setProperty("broker.id", Integer.toString(i));
      props.setProperty("auto.create.topics.enable", "true");
      props.setProperty("log.dirs", kafkaLogDir + "/" + i);
      props.setProperty("zookeeper.connect", zookeeperConnectURL);
      props.setProperty("zk.sessiontimeout.ms", "5000");
      props.setProperty("default.replication.factor", "2");
      props.setProperty("controlled.shutdown.enable", "true");
      server = new KafkaServer(new KafkaConfig(props), new MockTime());
      server.startup();
      broker = new Broker(i, "127.0.0.1", kafkaListenPort + i);
      kafkaServers.add(server);
      kafkaServers2.add(broker);
    }
    return kafkaServers;
  }

  public void stopZookeeper() throws IOException {
    if (testingServer == null)
      return;
    testingServer.stop();
    testingServer = null;
  }

  public void stopKafka() {
    for (KafkaServer kafkaServer : kafkaServers) {
      kafkaServer.shutdown();
      kafkaServer.awaitShutdown();
      kafkaServer = null;
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
      Thread.sleep(10000);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void injectTestData(String topic) {

    Properties props = new Properties();
    props.put("metadata.broker.list", getKafkaURLs());
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    props.put("producer.type", "async");
    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);

    for (int events = 0; events < 9999; events++) {
      String msg = UUID.randomUUID().toString();
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(
          topic, msg);
      try {
        producer.send(data);
      } catch (Exception c) {
        c.printStackTrace();
      }
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
    logger.debug("metadatabrokerlist " + builder.substring(0, builder.length() - 1));
    return builder.substring(0, builder.length() - 1);
  }

  public static void main(String[] args) {
    SampleKafkaCluster cluster = new SampleKafkaCluster(2182, 3, 2191, "~/tmp/test/tryzeks");
    /* cluster.setUseExternalZk(true);
     cluster.setZookeeperConnectURL("192.168.33.33:2181");*/
    cluster.start();
  }
}
