package com.neverwinterdp.scribengin.kafka.source;


import java.util.List;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.kafka.KafkaClient;
import com.neverwinterdp.scribengin.kafka.sink.KafkaWriter;
import com.neverwinterdp.server.kafka.KafkaCluster;
import com.neverwinterdp.vm.client.shell.Shell;

public class KafkaPartitionReaderUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/cluster", 1, 1);
    cluster.setNumOfPartition(5);
    cluster.start();
    Thread.sleep(2000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testReader() throws Exception {
    KafkaWriter writer = new KafkaWriter("writer", cluster.getKafkaConnect());
    for(int i = 0; i < 10; i++) {
      String hello = "Hello " + i;
      writer.send("hello", hello);
    }
    writer.close();
    
    Shell shell = newShell() ;
    shell.execute("registry dump --path /");
    
    System.out.println("...............................");
    KafkaClient kafkaClient = new KafkaClient("test", cluster.getZKConnect());
    kafkaClient.connect();
    TopicMetadata topicMetadata = kafkaClient.findTopicMetadata("hello");
    PartitionMetadata partitionMetadata = topicMetadata.partitionsMetadata().get(0);
    KafkaPartitionReader partitionReader = new KafkaPartitionReader("reader-name", "hello", partitionMetadata);
    
    try {
      List<byte[]> messages = partitionReader.fetch(100000);
      for(int i = 0; i < messages.size(); i++) {
        byte[] message = messages.get(i) ;
        System.out.println((i + 1) + ". " + new String(message));
      }
    } catch (Exception e) {
      System.out.println("Oops:" + e);
      e.printStackTrace();
    }
  }
  
  protected Shell newShell() throws RegistryException {
    RegistryConfig config = new RegistryConfig();
    config.setConnect("127.0.0.1:2181");
    config.setDbDomain("/brokers");
    config.setRegistryImplementation(RegistryImpl.class.getName());
    return new Shell(new RegistryImpl(config).connect()) ;
  }
}
