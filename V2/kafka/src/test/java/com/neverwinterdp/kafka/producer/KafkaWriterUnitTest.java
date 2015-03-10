package com.neverwinterdp.kafka.producer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
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
public class KafkaWriterUnitTest {
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

  /**
   * This unit test show that the kafka producer loose the messages when while one process send the messages continuosly
   * and another process shutdown the kafka partition leader
   * 
   * REF: http://qnalist.com/questions/5034216/lost-messages-during-leader-election
   * REF: https://issues.apache.org/jira/browse/KAFKA-1211
   * @throws Exception
   */
  @Test
  public void kafkaProducerLooseMessageWhenThePartitionLeaderShutdown() throws Exception {
    Map<String, String> kafkaProps = new HashMap<String, String>();
    kafkaProps.put("message.send.max.retries", "5");
    kafkaProps.put("retry.backoff.ms", "100");
    kafkaProps.put("queue.buffering.max.ms", "1000");
    kafkaProps.put("queue.buffering.max.messages", "15000");
    //kafkaProps.put("request.required.acks", "-1");
    kafkaProps.put("topic.metadata.refresh.interval.ms", "-1"); //negative value will refresh on failure
    kafkaProps.put("batch.num.messages", "100");
    kafkaProps.put("producer.type", "sync");
    //new config:
    kafkaProps.put("acks", "all");
    
    DefaultKafkaWriter writer = new DefaultKafkaWriter(NAME, kafkaProps, cluster.getKafkaConnect());
    int NUM_OF_SENT_MESSAGES = 5000 ;
    for(int i = 0; i < NUM_OF_SENT_MESSAGES; i++) {
      
      //Use this send to print out more detail about the message lost
      //writer.send("test", 0, "key-" + i, "test-1-" + i, new FailedAckReportCallback("message " + i) );
      writer.send("test", 0, "key-" + i, "test-1-" + i, 5000);
      //After sending 10 messages we shutdown and continue sending
      if(i == 10) {
        KafkapartitionLeaderKiller leaderKiller = new KafkapartitionLeaderKiller("test", 0);
        new Thread(leaderKiller).start();
        //IF we use the same writer thread to shutdown the leader and resume the sending. No message are lost
        //leaderKiller.run();
      }
    }
    writer.close();
    System.out.println("send done...");
    
    try {
      MessageConsumerCheckTool checkTool = new MessageConsumerCheckTool("test", NUM_OF_SENT_MESSAGES);
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
        partitionReader[i] = new KafkaPartitionReader(NAME, cluster.getZKConnect(), "test", partitionMetas.get(i));
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
          System.out.println("Check Tool: Cannot read more than " + messageCount + " messages");
        }
      }
      System.out.println("Check Tool: number of the consumed messages " + messageCount);
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
  
  class MessageFailDebugCallback implements Callback {
    private String description ;
    MessageFailDebugCallback(String desc) {
      this.description = desc;
    }
    
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if(exception != null) {
        System.err.println(description + ". Message  failed due to " + exception.getMessage());
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