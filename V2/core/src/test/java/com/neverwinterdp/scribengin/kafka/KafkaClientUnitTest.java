package com.neverwinterdp.scribengin.kafka;


import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.scribengin.kafka.sink.KafkaSink;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;
import com.neverwinterdp.server.kafka.KafkaCluster;

public class KafkaClientUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/cluster", 1, 1);
    cluster.start();
    Thread.sleep(2000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testKafkaClient() throws Exception {
    KafkaSink sink = new KafkaSink("writer", cluster.getZKConnect(), "hello");
    SinkStream stream = sink.newStream();
    SinkStreamWriter writer = stream.getWriter();
    for(int i = 0; i < 10; i++) {
    }
    writer.close();
    
    KafkaClient client = new KafkaClient("test", "127.0.0.1:2181");
    client.connect();
    ZooKeeper zkClient = client.getZookeeper();
    ZookeeperUtil.dump(zkClient, "/brokers");
    client.close();
  }
}
