package com.neverwinterdp.kafka.tool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Stopwatch;
import com.neverwinterdp.kafka.producer.AckKafkaWriter;
import com.neverwinterdp.kafka.producer.DefaultKafkaWriter;
import com.neverwinterdp.kafka.producer.KafkaWriter;
import com.neverwinterdp.kafka.tool.KafkaTopicReport.ProducerReport;

public class KafkaMessageSendTool implements Runnable {
  @ParametersDelegate
  private KafkaTopicConfig topicConfig = new KafkaTopicConfig();

  private Thread deamonThread;
  private boolean running = false;
  private AtomicLong   sendCounter = new AtomicLong() ;
  private KafkaMessageGenerator messageGenerator = new KafkaMessageGenerator();

  Map<Integer, PartitionMessageWriter> writers = new HashMap<Integer, PartitionMessageWriter>();
  private Stopwatch runDuration = Stopwatch.createUnstarted();

  public KafkaMessageSendTool() {
  }

  public KafkaMessageSendTool(KafkaTopicConfig topicConfig) {
    this.topicConfig = topicConfig;
  }
  
  public void setMessageGenerator(KafkaMessageGenerator generator) {
    messageGenerator = generator;
  }

  public boolean isSending() { return sendCounter.get() > 0 ; }
  
  public void report(KafkaTopicReport report) {
    ProducerReport producerReport = report.getProducerReport();
    producerReport.setWriter(topicConfig.producerConfig.writerType);
    producerReport.setMessageSize(topicConfig.producerConfig.messageSize);
    producerReport.setRunDuration(runDuration.elapsed(TimeUnit.MILLISECONDS));
    int messageSent = 0;// get all message senders, get writeCount from all
    for (PartitionMessageWriter writer : writers.values()) {
      messageSent += writer.writeCount;
    }
    producerReport.setMessageSent(messageSent);
    //TODO add failed
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
    if (kafkaTool.topicExits(topicConfig.topic)) {
      kafkaTool.deleteTopic(topicConfig.topic);
    }
    kafkaTool.createTopic(topicConfig.topic, topicConfig.replication, topicConfig.numberOfPartition);
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

    PartitionMessageWriter(PartitionMetadata metadata, String kafkaConnects) {
      this.metadata = metadata;
      this.kafkaConnects = kafkaConnects;
    }

    public int getWriteCount() { return this.writeCount ; }
    
    @Override
    public void run() {
      KafkaWriter writer = createKafkaWriter();
      try {
        boolean terminated = false;
        while (!terminated) {
          byte[] key = ("p:" + metadata.partitionId() + ":" + writeCount).getBytes();
          byte[] message = messageGenerator.nextMessage(metadata.partitionId(), topicConfig.producerConfig.messageSize) ;
          writer.send(topicConfig.topic, metadata.partitionId(), key, message, null, topicConfig.producerConfig.sendTimeout);
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

  static public void main(String[] args) throws Exception {
    KafkaMessageSendTool tool = new KafkaMessageSendTool();
    new JCommander(tool, args);
    tool.run();
  }
}
