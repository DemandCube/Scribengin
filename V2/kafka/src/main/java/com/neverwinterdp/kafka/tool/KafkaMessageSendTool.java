package com.neverwinterdp.kafka.tool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.neverwinterdp.kafka.producer.DefaultKafkaWriter;

public class KafkaMessageSendTool implements Runnable {
  @ParametersDelegate
  private KafkaConfig.Topic    topicConfig = new KafkaConfig.Topic();
  
  @ParametersDelegate
  private KafkaConfig.Producer senderConfig = new KafkaConfig.Producer();
  
  private Thread               deamonThread;
  private boolean              running      = false;  
  
  public KafkaMessageSendTool() { }
  
  public KafkaMessageSendTool(KafkaConfig.Topic topicConfig, KafkaConfig.Producer senderConfig) {
    this.topicConfig = topicConfig;
    this.senderConfig = senderConfig;
  }
  
  public void report(KafkaReport report) {
    //TODO: populate the report.producer variable
  }
  
  synchronized public boolean waitForTermination() throws InterruptedException {
    if(!running) return !running;
    wait(senderConfig.maxDuration);
    return !running;
  }
  
  synchronized void notifyTermination() {
    notifyAll() ;
  }
 
  public void runAsDeamon() {
    if(deamonThread != null && deamonThread.isAlive()) {
      throw new RuntimeException("Deamon thread is already started") ;
    }
    deamonThread = new Thread(this);
    deamonThread.start();
  }
  
  public void run() {
    running = true;
    try {
      doSend() ;
    } catch (Exception e) {
      e.printStackTrace();
    }
    running = false;
    notifyTermination();
  }
  
  public void doSend() throws Exception {
    Map<Integer, PartitionMessageWriter> writers = new HashMap<Integer, PartitionMessageWriter>();
    ExecutorService writerService = Executors.newFixedThreadPool(topicConfig.numberOfPartition);
    
    KafkaTool kafkaTool = new KafkaTool("KafkaTool", topicConfig.zkConnect);
    kafkaTool.connect();
    String kafkaConnects = kafkaTool.getKafkaBrokerList();
    if(kafkaTool.topicExits(topicConfig.topic)) {
      kafkaTool.deleteTopic(topicConfig.topic);
    }
    kafkaTool.createTopic(topicConfig.topic, topicConfig.replication, topicConfig.numberOfPartition);
    
    TopicMetadata topicMetadata = kafkaTool.findTopicMetadata(topicConfig.topic);
    List<PartitionMetadata> partitionMetadataHolder = topicMetadata.partitionsMetadata();
    for(PartitionMetadata sel : partitionMetadataHolder) {
      PartitionMessageWriter writer = new PartitionMessageWriter(sel, kafkaConnects);
      writers.put(sel.partitionId(), writer);
      writerService.submit(writer);
    }
    writerService.shutdown();
    writerService.awaitTermination(senderConfig.maxDuration, TimeUnit.MILLISECONDS);
    if(!writerService.isTerminated()) {
      writerService.shutdownNow();
    }
    kafkaTool.close();
  }
  
  public class PartitionMessageWriter implements Runnable {
    private PartitionMetadata metadata;
    private String kafkaConnects ;
    private int writeCount = 0;
    
    PartitionMessageWriter(PartitionMetadata metadata, String kafkaConnects) {
      this.metadata = metadata;
      this.kafkaConnects = kafkaConnects;
    }
    
    public void run() {
      DefaultKafkaWriter writer = new DefaultKafkaWriter("KafkaMessageSendTool", senderConfig.producerProperties, kafkaConnects);
      try {
        byte[] message = new byte[senderConfig.messageSize];
        boolean terminated = false ;
        while(!terminated) {
          byte[] key = ("p:" + metadata.partitionId() + ":" + writeCount).getBytes() ;
          writer.send(topicConfig.topic,  metadata.partitionId(), key, message, null, senderConfig.sendTimeout);
          writeCount++;
          //Check max message per partition
          if(writeCount >= senderConfig.maxMessagePerPartition) {
            terminated = true;
          } else if(senderConfig.sendPeriod > 0) {
            Thread.sleep(senderConfig.sendPeriod);
          }
        }
      } catch (InterruptedException e) {
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if(writer != null) {
          writer.close();
        }
      }
    }
  }
  
  static public void main(String[] args) throws Exception {
    KafkaMessageSendTool tool = new KafkaMessageSendTool();
    new JCommander(tool, args);
    tool.run();
  }
}
