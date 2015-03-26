package com.neverwinterdp.kafka.tool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Stopwatch;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.kafka.producer.DefaultKafkaWriter;
import com.neverwinterdp.kafka.producer.KafkaWriter;
import com.neverwinterdp.kafka.tool.KafkaTopicReport.ProducerReport;
import com.neverwinterdp.tool.message.MessageGenerator;

public class KafkaMessageSendTool implements Runnable {
  @ParametersDelegate
  private KafkaTopicConfig topicConfig = new KafkaTopicConfig();

  private Thread deamonThread;
  private boolean running = false;
  private AtomicLong   sendCounter = new AtomicLong() ;
  private AtomicLong   retried = new AtomicLong() ;
  private MessageGenerator messageGenerator = new MessageGenerator.DefaultMessageGenerator();

  Map<Integer, PartitionMessageWriter> writers = new HashMap<Integer, PartitionMessageWriter>();
  private Stopwatch runDuration = Stopwatch.createUnstarted();

  public KafkaMessageSendTool() {
  }
  
  public KafkaMessageSendTool(String[] args) {
    new JCommander(this, args);
  }

  public KafkaMessageSendTool(KafkaTopicConfig topicConfig) {
    this.topicConfig = topicConfig;
  }
  
  public void setMessageGenerator(MessageGenerator generator) {
    messageGenerator = generator;
  }

  public long getSentCount() { return sendCounter.get(); }
  
  public long getSentFailedCount() { return retried.get(); }
  
  public boolean isSending() { return sendCounter.get() > 0 ; }
  
  public KafkaTopicReport getReport() {
    KafkaTopicReport topicReport = new KafkaTopicReport() ;
    topicReport.setTopic(topicConfig.topic);
    topicReport.setNumOfPartitions(topicConfig.numberOfPartition);
    topicReport.setNumOfReplicas(topicConfig.replication);
    populate(topicReport) ;
    return topicReport ;
  }
  
  public void populate(KafkaTopicReport report) {
    ProducerReport producerReport = report.getProducerReport();
    producerReport.setWriter(topicConfig.producerConfig.writerType);
    producerReport.setMessageSize(topicConfig.producerConfig.messageSize);
    producerReport.setRunDuration(runDuration.elapsed(TimeUnit.MILLISECONDS));
    int messageSent = 0;
    for (PartitionMessageWriter writer : writers.values()) {
      messageSent += writer.writeCount;
    }
    producerReport.setMessageSent(messageSent);
    producerReport.setMessageRetried(retried.get());;
  }

  
  synchronized public boolean waitForTermination() throws InterruptedException {
    if (!running) return !running;
    wait(topicConfig.producerConfig.maxDuration);
    return !running;
  }

  synchronized void notifyTermination() {
    notifyAll();
  }

  public void runAsDeamon() {
    if (deamonThread != null && deamonThread.isAlive()) {
      throw new RuntimeException("Deamon thread is already started");
    }
    deamonThread = new Thread(this);
    deamonThread.start();
  }

  public void run() {
    running = true;
    try {
      doSend();
    } catch (Exception e) {
      e.printStackTrace();
    }
    running = false;
    notifyTermination();
  }

  public void doSend() throws Exception {
    System.out.println("KafkaMessageSendTool: Start sending the message to kafka");
    runDuration.start();
    ExecutorService writerService = Executors.newFixedThreadPool(topicConfig.numberOfPartition);
    KafkaTool kafkaTool = new KafkaTool("KafkaTool", topicConfig.zkConnect);
    kafkaTool.connect();
    String kafkaConnects = kafkaTool.getKafkaBrokerList();
    //TODO: add option to delete topic if it exists
    //kafkaTool.deleteTopic(topicConfig.topic);
    if(!kafkaTool.topicExits(topicConfig.topic)) {
      kafkaTool.createTopic(topicConfig.topic, topicConfig.replication, topicConfig.numberOfPartition);
    }
   
    TopicMetadata topicMetadata = kafkaTool.findTopicMetadata(topicConfig.topic);
    List<PartitionMetadata> partitionMetadataHolder = topicMetadata.partitionsMetadata();
    for (PartitionMetadata sel : partitionMetadataHolder) {
      PartitionMessageWriter writer = new PartitionMessageWriter(sel, kafkaConnects);
      writers.put(sel.partitionId(), writer);
      writerService.submit(writer);
    }

    writerService.shutdown();
    writerService.awaitTermination(topicConfig.producerConfig.maxDuration, TimeUnit.MILLISECONDS);
    if (!writerService.isTerminated()) {
      writerService.shutdownNow();
    }
    kafkaTool.close();
    runDuration.stop();
  }

  public class PartitionMessageWriter implements Runnable {
    private PartitionMetadata metadata;
    private String kafkaConnects;
    //TODO atomic integer for thread safety
    private int writeCount = 0;
    private MessageFailedCountCallback failedCountCallback ;

    PartitionMessageWriter(PartitionMetadata metadata, String kafkaConnects) {
      this.metadata = metadata;
      this.kafkaConnects = kafkaConnects;
      failedCountCallback = new MessageFailedCountCallback();
    }

    public int getWriteCount() { return this.writeCount ; }
    
    @Override
    public void run() {
      KafkaWriter writer = createKafkaWriter();
      try {
        boolean terminated = false;
        while (!terminated) {
          //System.err.println("Partition id: "+Integer.toString(metadata.partitionId())+" - Write count: "+Integer.toString(writeCount));
          byte[] key = ("p:" + metadata.partitionId() + ":" + writeCount).getBytes();
          byte[] message = messageGenerator.nextMessage(metadata.partitionId(), topicConfig.producerConfig.messageSize) ;
          writer.send(topicConfig.topic, metadata.partitionId(), key, message, failedCountCallback, topicConfig.producerConfig.sendTimeout);
          writeCount++;
          sendCounter.incrementAndGet();
          //Check max message per partition
          if (writeCount >= topicConfig.producerConfig.maxMessagePerPartition) {
            terminated = true;
          } else if (topicConfig.producerConfig.sendPeriod > 0) {
            Thread.sleep(topicConfig.producerConfig.sendPeriod);
          }
        }
      } catch (InterruptedException e) {
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (writer != null) {
          try {
            Thread.sleep(1000); //wait to make sure writer flush or handle all the messages in the buffer
            writer.close();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    }
    
    private KafkaWriter createKafkaWriter() {
      if("ack".equalsIgnoreCase(topicConfig.producerConfig.writerType)) {
        return new AckKafkaWriter("KafkaMessageSendTool", topicConfig.producerConfig.producerProperties, kafkaConnects);
      } else {
        return new DefaultKafkaWriter("KafkaMessageSendTool", topicConfig.producerConfig.producerProperties, kafkaConnects);
      }
    }
  }
  
  class MessageFailedCountCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null)
        retried.incrementAndGet();
    }
  }

  static public void main(String[] args) throws Exception {
    KafkaMessageSendTool tool = new KafkaMessageSendTool();
    new JCommander(tool, args);
    tool.run();
  }
}
