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

public class NizarKafkaProducerUnitTest {
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
    kafkaProps.put("retry.backoff.ms", "300");
    
    final KafkaTool kafkaTool = new KafkaTool("test", cluster.getZKConnect());
    kafkaTool.connect();
    KafkaWriter writer = new KafkaWriter("test", kafkaProps, cluster.getKafkaConnect());
    //send 10 000 messages
    for(int i = 0; i < 10000; i++) {
      writer.send("test", "test-1-" + i);
      //after sending 10 messages we shutdown and continue sending
      //this will make the producer fails after sending about 30 messages
      if(i == 10){
        new Thread(){
          public void run(){

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
        }.start();
      }
    }
    System.out.println("Send before leader shutdown");
 

    System.out.println("Send after leader shutdown");
    writer.close();
    kafkaTool.close();
    System.out.println("send done...");
    
    kafkaTool.connect();
    TopicMetadata topicMeta = kafkaTool.findTopicMetadata("test");
    PartitionMetadata partitionMeta = topicMeta.partitionsMetadata().get(0);
    KafkaPartitionReader partitionReader = new KafkaPartitionReader("test", "test", partitionMeta);
    List<byte[]> messages = partitionReader.fetch(10000, 30);
    for(int i = 0; i < messages.size(); i++) {
      byte[] message = messages.get(i) ;
      System.out.println((i + 1) + ". " + new String(message));
    }
    partitionReader.commit();
    partitionReader.close();

    kafkaTool.close();
  }
}

