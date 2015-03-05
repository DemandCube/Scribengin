package com.neverwinterdp.kafka.producer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.kafka.KafkaCluster;

public class NizarKafkaProducerResearchTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/kafka", 1, 2);
    cluster.setReplication(2);
    cluster.setNumOfPartition(1);
    cluster.start();
    Thread.sleep(2000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testKafkaProducer() throws Exception {
    Map<String, String> kafkaProps = new HashMap<String, String>();
    kafkaProps.put("message.send.max.retries", "5");
    kafkaProps.put("retry.backoff.ms", "100");
    kafkaProps.put("queue.buffering.max.ms", "1000");
    kafkaProps.put("queue.buffering.max.messages", "5000");
    kafkaProps.put("request.required.acks", "-1");
    kafkaProps.put("topic.metadata.refresh.interval.ms", "-1"); //negative value will refresh on failure
    kafkaProps.put("batch.num.messages", "10");
    kafkaProps.put("producer.type", "sync");
    
    final KafkaTool kafkaTool = new KafkaTool("test", cluster.getZKConnect());
    kafkaTool.connect();
    KafkaWriter writer = new KafkaWriter("test", kafkaProps, cluster.getKafkaConnect());
    Runnable kafkaKiller = new Runnable() {
      public void run() {
        try {
          TopicMetadata topicMeta = kafkaTool.findTopicMetadata("test");
          PartitionMetadata partitionMeta = topicMeta.partitionsMetadata().get(0);
          Broker partitionLeader = partitionMeta.leader() ;
          Server kafkaServer = cluster.findKafkaServerByPort(partitionLeader.port());
          System.out.println("Shutdown kafka server " + kafkaServer.getPort());
          kafkaServer.shutdown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    //send 10 000 messages
    for(int i = 0; i < 10000; i++) {
      writer.send("test", 0, "key-" + i, "test-1-" + i);
      //after sending 10 messages we shutdown and continue sending
      //this will make the producer fails after sending about 30 messages
      if(i == 10){
        new Thread(kafkaKiller).start();
        //kafkaKiller.run();
      }
    }
    System.out.println("Send before leader shutdown");
 
    Thread.sleep(3000);
    System.out.println("Send after leader shutdown");
    writer.close();
    kafkaTool.close();
    System.out.println("send done...");
    
    kafkaTool.connect();
    TopicMetadata topicMeta = kafkaTool.findTopicMetadata("test");
    PartitionMetadata partitionMeta = topicMeta.partitionsMetadata().get(0);
    KafkaPartitionReader partitionReader = new KafkaPartitionReader("test", "test", partitionMeta);
    int count = 0;
    while(count < 10000) {
      List<byte[]> messages = partitionReader.fetch(10000, 100);
      for(int i = 0; i < messages.size(); i++) {
        byte[] message = messages.get(i) ;
        count++;
      }
      System.out.println("count = " + count);
    }
    partitionReader.commit();
    partitionReader.close();
    kafkaTool.close();
  }
}