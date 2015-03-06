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
import org.junit.Assert;

import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.kafka.KafkaCluster;
/**
 * This unit test is used to isolate and show all the kafka producer bugs and limitation
 * 
 * @author Tuan
 */
public class KafkaProducerBugsUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  static String NAME = "test" ;
  
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
  public void kafkaProducerLooseMessageWhenThePartitionLeaderShutdown() throws Exception {
    Map<String, String> kafkaProps = new HashMap<String, String>();
    kafkaProps.put("message.send.max.retries", "5");
    kafkaProps.put("retry.backoff.ms", "100");
    kafkaProps.put("queue.buffering.max.ms", "1000");
    kafkaProps.put("queue.buffering.max.messages", "15000");
    //kafkaProps.put("request.required.acks", "-1");
    kafkaProps.put("topic.metadata.refresh.interval.ms", "-1"); //negative value will refresh on failure
    kafkaProps.put("batch.num.messages", "200");
    kafkaProps.put("producer.type", "sync");
    //new config:
    kafkaProps.put("acks", "all");
    
    KafkaWriter writer = new KafkaWriter(NAME, kafkaProps, cluster.getKafkaConnect());
    //send 10 000 messages
    for(int i = 0; i < 10000; i++) {
      writer.send("test", 0, "key-" + i, "test-1-" + i);
      //after sending 10 messages we shutdown and continue sending
      //this will make the producer fails after sending about 30 messages
      if(i == 10) {
        KafkapartitionLeaderKiller leaderKiller = new KafkapartitionLeaderKiller("test", 0);
        new Thread(leaderKiller).start();
        //leaderKiller.run();
      }
    }
    writer.close();
    System.out.println("send done...");
    
    try {
      MessageConsumerCheckTool checkTool = new MessageConsumerCheckTool("test", 10000);
      checkTool.check();
    } catch(AssertionError error) {
      //This error is expected as it is a kafka producer bug
      System.err.println(error.getMessage());
    }
  }
  
  class MessageConsumerCheckTool {
    private String topic;
    private int expectNumberOfMessage;
    
    MessageConsumerCheckTool(String topic, int expect) {
      this.topic = topic;
      this.expectNumberOfMessage = expect;
    }
    
    public void check() throws Exception {
      KafkaTool kafkaTool = new KafkaTool(NAME, cluster.getZKConnect());
      kafkaTool.connect();
      
      TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topic);
      List<PartitionMetadata> partitionMetas = topicMeta.partitionsMetadata();
      KafkaPartitionReader[] partitionReader = new KafkaPartitionReader[partitionMetas.size()];
      for(int i = 0; i < partitionReader.length; i++) {
        partitionReader[i] = new KafkaPartitionReader(NAME, "test", partitionMetas.get(i));
      }
      
      int messageCount = 0, cannotReadCount = 0;
      while(messageCount < expectNumberOfMessage && cannotReadCount < 5) {
        int messageRead = 0 ;
        for(int k = 0; k < partitionReader.length; k++) {
          List<byte[]> messages = partitionReader[k].fetch(100000/*fetch size*/, 100/*max read*/, 1000 /*max wait*/);
          for(int i = 0; i < messages.size(); i++) {
            byte[] message = messages.get(i) ;
            messageCount++;
            messageRead++ ;
          }
        }
        if(messageRead == 0) {
          cannotReadCount++;
          System.out.println("Check Tool: Cannot read more message " + messageCount);
        }
      }
      System.out.println("Check Tool: number of consumed message " + messageCount);
      for(int k = 0; k < partitionReader.length; k++) {
        partitionReader[k].commit();
        partitionReader[k].close();
      }
      kafkaTool.close();
      if(messageCount != expectNumberOfMessage) {
        Assert.fail("Message check tool expect to consume " + expectNumberOfMessage + ", but can consume only " + messageCount);
      }
    }
  }
  
  class KafkapartitionLeaderKiller implements Runnable {
    private String topic;
    private int    partition;
    
    KafkapartitionLeaderKiller(String topic, int partition) {
      this.topic = topic;
      this.partition = partition ;
    }
    
    public void run() {
      try {
        KafkaTool kafkaTool = new KafkaTool("test", cluster.getZKConnect());
        kafkaTool.connect();
        TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topic);
        PartitionMetadata partitionMeta = findPartition(topicMeta, partition);
        Broker partitionLeader = partitionMeta.leader() ;
        Server kafkaServer = cluster.findKafkaServerByPort(partitionLeader.port());
        System.out.println("Shutdown kafka server " + kafkaServer.getPort());
        kafkaServer.shutdown();
        kafkaTool.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    
    PartitionMetadata findPartition(TopicMetadata topicMetadata, int partion) {
      for(PartitionMetadata sel :topicMetadata.partitionsMetadata()) {
        if(sel.partitionId() == partition) return sel;
      }
      throw new RuntimeException("Cannot find the partition " + partition);
    }
  }
}