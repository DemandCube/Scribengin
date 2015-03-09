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
import com.neverwinterdp.util.FileUtil;
/**
 * @author Tuan
 */
public class AckKafkaWriterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  static String NAME = "test" ;
  
  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    FileUtil.removeIfExist("./build/kafka", false);
    cluster = new KafkaCluster("./build/kafka", 1, 3);
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
    kafkaProps.put("request.required.acks", "-1");
    kafkaProps.put("topic.metadata.refresh.interval.ms", "-1"); //negative value will refresh on failure
    kafkaProps.put("batch.num.messages", "200");
    kafkaProps.put("producer.type", "async");
    //new config:
    //kafkaProps.put("acks", "all");

    String TOPIC = "test";
    int    NUM_OF_SENT_MESSAGES = 100000;
    int    MESSAGE_SIZE = 4092;
    
    KafkaTool kafkaTool = new KafkaTool(TOPIC, cluster.getZKConnect());
    kafkaTool.connect();
    kafkaTool.createTopic(TOPIC, 3, 5);
    kafkaTool.close();
    
    KafkaMessageSendTool sendTool = new KafkaMessageSendTool(TOPIC, NUM_OF_SENT_MESSAGES, MESSAGE_SIZE);
    new Thread(sendTool).start();
    
    KafkapartitionLeaderKiller leaderKiller = new KafkapartitionLeaderKiller(TOPIC, 0, 3000);
    new Thread(leaderKiller).start();
    
    sendTool.waitTermination(300000); // send for max 5 mins
    leaderKiller.exit();
    //make sure that no server shutdown when run the check tool
    //The check tool is not designed to read from the broken server env
    leaderKiller.waitForTermination(30000); 
    System.out.println("====================Leader Killer should stop here=====================");
    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool(cluster.getZKConnect(), TOPIC, sendTool.getNumOfSentMessages());
    checkTool.setFetchSize(MESSAGE_SIZE * 125); 
    checkTool.runAsDeamon();
    checkTool.waitForTermination(300000);
    checkTool.getMessageCounter().print(System.out, "Topic: " + TOPIC);
    System.out.println("Num Of Sent Messages: " + sendTool.getNumOfSentMessages());
    System.out.println("Failure simulate count: " + leaderKiller.getFaillureCount());
  }
  
  class KafkaMessageSendTool implements Runnable {
    private String  topic ;
    private int     maxNumOfMessages = 10000 ;
    private int     messageSize   = 1024  ;
    private long    waitBeforeClose = 10000;
    private int     numOfSentMessages = 0;
    private boolean exit = false;
    
    
    public KafkaMessageSendTool(String topic, int maxNumOfMessages, int messageSize) {
      this.topic = topic;
      this.maxNumOfMessages = maxNumOfMessages;
      this.messageSize = messageSize;
    }
    
    public int getNumOfSentMessages() { return this.numOfSentMessages; }
    
    @Override
    public void run() {
      try {
        runSend();
      } catch (Exception e) {
        e.printStackTrace();
      }
      notifyTermination(); 
    }
    
    void runSend() throws Exception {
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
      
      AckKafkaWriter writer = new AckKafkaWriter("KafkaMessageSendTool", kafkaProps, cluster.getKafkaConnect());
      while(!exit && numOfSentMessages < maxNumOfMessages) {
        //Use this send to print out more detail about the message lost
        byte[] key = ("key-" + numOfSentMessages).getBytes();
        byte[] message = new byte[messageSize];
        writer.send(topic, key, message, new MessageFailDebugCallback("message " + numOfSentMessages), 10000);
        numOfSentMessages++ ;
      }
      writer.waitAndClose(waitBeforeClose);
    }
    
    synchronized public void notifyTermination() {
      notify();
    }
    
    synchronized public void waitTermination(long timeout) throws InterruptedException {
      wait(timeout);
      exit = true;
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
    private String  topic;
    private int     partition;
    private long    sleepBeforeRestart = 500;
    private int     failureCount;
    private boolean exit = false;

    KafkapartitionLeaderKiller(String topic, int partition, long sleepBeforeRestart) {
      this.topic = topic;
      this.partition = partition ;
      this.sleepBeforeRestart = sleepBeforeRestart;
    }
    
    public int getFaillureCount() { return this.failureCount; }
    
    public void exit() { exit = true; }
    
    public void run() {
      try {
        while(!exit) {
          KafkaTool kafkaTool = new KafkaTool(topic, cluster.getZKConnect());
          kafkaTool.connect();
          TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topic);
          PartitionMetadata partitionMeta = findPartition(topicMeta, partition);
          Broker partitionLeader = partitionMeta.leader() ;
          Server kafkaServer = cluster.findKafkaServerByPort(partitionLeader.port());
          System.out.println("Shutdown kafka server " + kafkaServer.getPort());
          kafkaServer.shutdown();
          failureCount++;
          Thread.sleep(sleepBeforeRestart);
          kafkaServer.start();
          kafkaTool.close();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      synchronized(this) {
        notify() ;
      }
    }
    
    PartitionMetadata findPartition(TopicMetadata topicMetadata, int partion) {
      for(PartitionMetadata sel :topicMetadata.partitionsMetadata()) {
        if(sel.partitionId() == partition) return sel;
      }
      throw new RuntimeException("Cannot find the partition " + partition);
    }
    
    synchronized public void waitForTermination(long timeout) throws InterruptedException {
      wait(timeout);
    }
  }
}