package com.neverwinterdp.kafka.producer;

import java.util.HashMap;
import java.util.Map;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.kafka.KafkaCluster;
/**
 * @author Tuan
 */
public class AckKafkaWriterUnitTest {
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
  public void testSend() throws Exception {
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
    
    AckKafkaWriter writer = new AckKafkaWriter(NAME, kafkaProps, cluster.getKafkaConnect());
    int NUM_OF_SENT_MESSAGES = 10000 ;
    int MESSAGE_SIZE = 750;
    for(int i = 0; i < NUM_OF_SENT_MESSAGES; i++) {
      //Use this send to print out more detail about the message lost
      byte[] key = ("key-" + i).getBytes();
      byte[] message = new byte[MESSAGE_SIZE];
      writer.send("test", 0, key, message, new MessageFailDebugCallback("message " + i), 30000);
      //After sending 10 messages we shutdown and continue sending
      if(i == 10) {
        KafkapartitionLeaderKiller leaderKiller = new KafkapartitionLeaderKiller("test", 0);
        new Thread(leaderKiller).start();
        //IF we use the same writer thread to shutdown the leader and resume the sending. No message are lost
        //leaderKiller.run();
      }
    }
    writer.waitAndClose(30000);;
    System.out.println("send done...");
    
    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool(cluster.getZKConnect(), "test", NUM_OF_SENT_MESSAGES);
    checkTool.runAsDeamon();
    checkTool.waitForTermination(20000);
    checkTool.getMessageCounter().print(System.out, "Topic: test");
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