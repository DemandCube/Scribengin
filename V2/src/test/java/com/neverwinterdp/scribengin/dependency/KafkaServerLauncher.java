package com.neverwinterdp.scribengin.dependency;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

import com.neverwinterdp.util.JSONSerializer;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class KafkaServerLauncher {
  private KafkaServer server ;
  ThreadGroup kafkaGroup ;
  int kafkaGroupTracker = 1 ;
  private Properties properties = new Properties();
  
  public KafkaServerLauncher(Map<String, String> overrideProperties) {
    init(overrideProperties) ;
  }
  
  public KafkaServerLauncher(int id, String dataDir) {
    Map<String, String> props = new HashMap<String, String>() ;
    props.put("broker.id", Integer.toString(id));
    props.put("log.dirs", dataDir);
    init(props) ;
  }
  
  void init(Map<String, String> overrideProperties) {
    properties.put("port", "9092");
    properties.put("broker.id", "1");
    properties.put("auto.create.topics.enable", "true");
    properties.put("log.dirs", "./build/data/kafka");
    //props.setProperty("enable.zookeeper", "true");
    properties.put("zookeeper.connect", "127.0.0.1:2181");
    properties.put("controlled.shutdown.enable", "true");
    properties.put("auto.leader.rebalance.enable", "true");
    if(overrideProperties != null) {
      properties.putAll(overrideProperties);
    }
  }
  
  public void start() throws Exception {
    kafkaGroup = new ThreadGroup("Kafka-" + properties.getProperty("broker.id") + "-" + ++kafkaGroupTracker) ;
    String logDir = properties.getProperty("log.dirs") ;
    logDir = logDir.replace("/", File.separator) ;
    properties.setProperty("log.dirs", logDir) ;
    
    System.out.println("kafka properties:\n" + JSONSerializer.INSTANCE.toString(properties));
    
    Thread thread = new Thread(kafkaGroup, "KafkaLauncher") {
      public void run() {
        server = new KafkaServer(new KafkaConfig(properties), new SystemTime());
        server.startup();
      }
    };
    thread.start() ;
    //Wait to make sure the server is launched
    Thread.sleep(1000);
  }

  public void stop() {
    if(server == null) return ;
    long startTime = System.currentTimeMillis() ;
    //server.awaitShutdown();
    //server.socketServer().shutdown();
    //server.kafkaController().shutdown();
    //server.kafkaScheduler().shutdown();
    //server.replicaManager().shutdown() ;
    //kafkaGroup.interrupt() ;
    server.shutdown();
    //server.kafkaController().shutdown();
    //server.replicaManager().replicaFetcherManager().closeAllFetchers();
    //server.kafkaScheduler().shutdown();
    //server.logManager().shutdown();
    kafkaGroup.interrupt() ;
    kafkaGroup = null ;
    server = null ;
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
}