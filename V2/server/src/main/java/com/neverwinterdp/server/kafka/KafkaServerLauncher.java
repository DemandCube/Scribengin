package com.neverwinterdp.server.kafka;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import com.neverwinterdp.server.Server;
import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class KafkaServerLauncher implements Server {
  private KafkaServer server;
  ThreadGroup kafkaGroup;
  int kafkaGroupTracker = 1;
  private Properties properties = new Properties();
  private Thread thread;

  public KafkaServerLauncher() {
    properties.put("port", "9092");
    properties.put("broker.id", "1");
    properties.put("auto.create.topics.enable", "true");
    properties.put("log.dirs", "./build/data/kafka");
    properties.put("enable.zookeeper", "true");
    properties.put("zookeeper.connect", "127.0.0.1:2181");
    properties.put("default.replication.factor", "1");
    
    properties.put("controlled.shutdown.enable", "true");
    properties.put("auto.leader.rebalance.enable", "true");
    properties.put("controller.socket.timeout.ms", "90000");
    properties.put("controlled.shutdown.enable", "true");
    properties.put("controlled.shutdown.max.retries", "3");
    properties.put("controlled.shutdown.retry.backoff.ms", "60000");
  }
  
  public KafkaServerLauncher(int id, String dataDir, int port) {
    this();
    properties.put("broker.id", Integer.toString(id));
    properties.put("port", Integer.toString(port));
    properties.put("log.dirs", dataDir);
  }

  public KafkaServerLauncher(Map<String, String> overrideProperties) {
    this();
    if(overrideProperties != null) {
      properties.putAll(overrideProperties);
    }
  }
  
  public KafkaServerLauncher setReplication(int replication) {
    properties.put("default.replication.factor", Integer.toString(replication));
    return this;
  }
  
  public KafkaServerLauncher setZkConnect(String zkConnect) {
    properties.put("zookeeper.connect", zkConnect);
    return this;
  }
  
  public KafkaServerLauncher setNumOfPartition(int number) {
    properties.put("num.partitions", Integer.toString(number));
    return this;
  }

  public void start() throws Exception {
    kafkaGroup =
        new ThreadGroup("Kafka-" + properties.getProperty("broker.id") + "-" + ++kafkaGroupTracker);
    String logDir = properties.getProperty("log.dirs");
    logDir = logDir.replace("/", File.separator);
    properties.setProperty("log.dirs", logDir);

    System.out.println("kafka properties:\n" + JSONSerializer.INSTANCE.toString(properties));
    thread = new Thread(kafkaGroup, "KafkaLauncher") {
      public void run() {
        server = new KafkaServer(new KafkaConfig(properties), new SystemTime());
        server.startup();
      }
    };
    thread.start();
    // Wait to make sure the server is launched
    Thread.sleep(2000);
  }

  @Override
  public void shutdown() {
    if (server == null)
      return;
    long startTime = System.currentTimeMillis();
    // server.awaitShutdown();
    // server.socketServer().shutdown();
    // server.kafkaController().shutdown();
    // server.kafkaScheduler().shutdown();
    // server.replicaManager().shutdown() ;
    // kafkaGroup.interrupt() ;
    server.shutdown();
    // server.kafkaController().shutdown();
    // server.replicaManager().replicaFetcherManager().closeAllFetchers();
    // server.kafkaScheduler().shutdown();
    // server.logManager().shutdown();
    kafkaGroup.interrupt();
    kafkaGroup = null;
    server = null;
    System.out.println("KafkaThreadKiller thread shutdown kafka successfully");
    System.out.println("Shutdown KafkaServer in " + (System.currentTimeMillis() - startTime) + "ms");
  }

  static public class SystemTime implements Time {
    public long milliseconds() {
      return System.currentTimeMillis();
    }

    public long nanoseconds() {
      return System.nanoTime();
    }

    public void sleep(long ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
      }
    }
  }

  @Override
  public String getHost() {
    return server.config().advertisedHostName();
  }

  @Override
  public int getPort() {
    return server.config().advertisedPort();
  }
}
