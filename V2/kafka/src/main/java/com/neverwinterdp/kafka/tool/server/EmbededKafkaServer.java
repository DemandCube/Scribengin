package com.neverwinterdp.kafka.tool.server;

import java.io.File;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import com.neverwinterdp.tool.server.Server;
import com.neverwinterdp.util.JSONSerializer;

/**
 * @author Tuan Nguyen
 * @email tuan08@gmail.com
 */
public class EmbededKafkaServer implements Server {
  private KafkaServer server;
  ThreadGroup kafkaGroup;
  int kafkaGroupTracker = 1;
  private Properties properties = new Properties();
  private Thread thread;
  private boolean verbose = true;
  
  public EmbededKafkaServer(int id, String dataDir, int port) {
    properties.put("host.name", "127.0.0.1");
    properties.put("advertised.host.name", "127.0.0.1");
    properties.put("port", "9092");
    properties.put("broker.id", "1");
    properties.put("auto.create.topics.enable", "true");
    properties.put("log.dirs", "./build/data/kafka");
    properties.put("zookeeper.connect", "127.0.0.1:2181");
    properties.put("default.replication.factor", "1");
    //enable topic deletion
    properties.put("delete.topic.enable", "true");

    properties.put("controlled.shutdown.enable", "true");
    properties.put("auto.leader.rebalance.enable", "true");
    properties.put("controller.socket.timeout.ms", "30000");
    properties.put("controlled.shutdown.enable", "true");
    properties.put("controlled.shutdown.max.retries", "3");
    properties.put("controlled.shutdown.retry.backoff.ms", "5000");
    properties.put("zookeeper.session.timeout.ms", "15000");
    
    properties.put("broker.id", Integer.toString(id));
    properties.put("port", Integer.toString(port));
    properties.put("log.dirs", dataDir);
  }

  public EmbededKafkaServer setVerbose(boolean b) {
    verbose = b ;
    return this;
  }
  
  public EmbededKafkaServer setReplication(int replication) {
    properties.put("default.replication.factor", Integer.toString(replication));
    return this;
  }

  public EmbededKafkaServer setZkConnect(String zkConnect) {
    properties.put("zookeeper.connect", zkConnect);
    return this;
  }

  public EmbededKafkaServer setNumOfPartition(int number) {
    properties.put("num.partitions", Integer.toString(number));
    return this;
  }

  public void start() throws Exception {
    kafkaGroup =
        new ThreadGroup("Kafka-" + properties.getProperty("broker.id") + "-" + ++kafkaGroupTracker);
    String logDir = properties.getProperty("log.dirs");
    logDir = logDir.replace("/", File.separator);
    properties.setProperty("log.dirs", logDir);

    if(verbose) {
      System.out.println("kafka properties:\n" + JSONSerializer.INSTANCE.toString(properties));
    }
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
    System.out.println("KafkaThreadKiller thread shutdown kafka successfully in " + (System.currentTimeMillis() - startTime) + "ms");
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
  
  @Override
  public String getConnectString() {
    return getHost() + ":" + getPort() ;
  }
}
