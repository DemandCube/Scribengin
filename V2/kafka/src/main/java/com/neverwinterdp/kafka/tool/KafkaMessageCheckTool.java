package com.neverwinterdp.kafka.tool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Stopwatch;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.kafka.tool.KafkaTopicReport.ConsumerReport;
import com.neverwinterdp.tool.message.MessageExtractor;
import com.neverwinterdp.tool.message.MessageTracker;
import com.neverwinterdp.util.text.TabularFormater;

public class KafkaMessageCheckTool implements Runnable {
  static private String NAME = "KafkaMessageCheckTool";

  @ParametersDelegate
  private KafkaTopicConfig topicConfig = new KafkaTopicConfig();
  
  private MessageExtractor messageExtractor = MessageExtractor.DEFAULT_MESSAGE_EXTRACTOR;
  private MessageTracker   messageTracker = new MessageTracker() ;
  private MessageCounter messageCounter = new MessageCounter();
  private int numOfPartitions = 0;
  private boolean interrupt = false;
  private Thread  deamonThread;
  private Stopwatch readDuration = Stopwatch.createUnstarted();
  private boolean running = false;
  
  public KafkaMessageCheckTool(String[] args) {
    new JCommander(this, args);
  }

  public KafkaMessageCheckTool(KafkaTopicConfig topicConfig) {
    this.topicConfig = topicConfig;
  }

  public void setMessageExtractor(MessageExtractor extractor) {
    this.messageExtractor = extractor ;
  }
  
  public MessageTracker getMessageTracker() { return messageTracker ; }
  
  public MessageCounter getMessageCounter() { return messageCounter; }

  public Stopwatch getReadDuration() { return readDuration; }

  public void setInterrupt(boolean b) { this.interrupt = b; }

  synchronized public boolean waitForTermination(long maxWaitTime) throws InterruptedException {
    if (!running) return !running;
    wait(maxWaitTime);
    return !running;
  }

  synchronized public boolean waitForTermination() throws InterruptedException {
    if(!running) return !running;
    wait(topicConfig.consumerConfig.maxDuration);
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
      check();
    } catch (Exception e) {
      e.printStackTrace();
    }
    running = false;
    notifyTermination();
  }

  //TODO each partition reader on a separate thread. same as SendTool
  public void check() throws Exception {
    System.out.println("KafkaMessageCheckTool: Start running kafka message check tool.");
    readDuration.start();
    KafkaTool kafkaTool = new KafkaTool(NAME, topicConfig.zkConnect);
    kafkaTool.connect();
    
    TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topicConfig.topic, 3);
    List<PartitionMetadata> partitionMetas = topicMeta.partitionsMetadata();
    numOfPartitions = partitionMetas.size();
    kafkaTool.close();
    
    
    interrupt = false;
    int batchFetch = topicConfig.consumerConfig.consumeBatchFetch ;
    int fetchSize = batchFetch * (topicConfig.producerConfig.messageSize + 100) ;
    
    ExecutorService executorService = Executors.newFixedThreadPool(numOfPartitions);

    KafkaPartitionConsumer[] partitionConsumer = new KafkaPartitionConsumer[numOfPartitions];
    for (int i = 0; i < partitionConsumer.length; i++) {
      KafkaPartitionReader partitionReader = 
          new KafkaPartitionReader(NAME, topicConfig.zkConnect, topicConfig.topic, partitionMetas.get(i));
      partitionConsumer[i] = 
        new KafkaPartitionConsumer(partitionReader, batchFetch, fetchSize);
      executorService.submit(partitionConsumer[i]);
    }
    executorService.shutdown();
    while(!executorService.isTerminated()) {
      System.out.println("Read count: " + messageCounter.getTotal());
      if(messageCounter.getTotal() - messageTracker.getDuplicatedCount() >= topicConfig.consumerConfig.consumeMax) {
        interrupt = true;
      }
      Thread.sleep(5000);
    }
    
    System.out.println("Read count: " + messageCounter.getTotal() +"(Stop)") ;
    messageTracker.optimize();
    readDuration.stop();
  }

  public KafkaTopicReport getReport() {
    KafkaTopicReport report = new KafkaTopicReport() ;
    report.setTopic(topicConfig.topic);
    report.setNumOfPartitions(numOfPartitions);
    report.setNumOfReplicas(topicConfig.replication);
    populate(report);
    return report ;
  }

  public void populate(KafkaTopicReport report) {
    ConsumerReport consumerReport = report.getConsumerReport();
    consumerReport.setMessagesRead(messageCounter.totalMessages.get());
    consumerReport.setRunDuration(readDuration.elapsed(TimeUnit.MILLISECONDS));
  }

  static public class MessageCounter {
    private Map<Integer, AtomicInteger> counters = new HashMap<>();
    private AtomicInteger totalMessages = new AtomicInteger();

    public int getTotal() {
      return totalMessages.get();
    }
    
    public Map<Integer, AtomicInteger> getCounter(){ return counters; }

    public int getPartitionCount(int partition) {
      return counters.get(partition).get();
    }

    synchronized public void count(int partition, int readMessage) {
      AtomicInteger current = counters.get(partition);
      if(current == null) {
        counters.put(partition, new AtomicInteger(readMessage));
      } else {
        current.addAndGet(readMessage);
      }
      totalMessages.addAndGet(readMessage);
    }

    public void print(Appendable out, String title) {
      TabularFormater formater = new TabularFormater("Partition", "Read");
      formater.setTitle(title + "(" + totalMessages + ")");

      formater.setIndent("  ");
      for (Map.Entry<Integer, AtomicInteger> entry : counters.entrySet()) {
        formater.addRow(entry.getKey(), entry.getValue().get());
      }

      try {
        out.append(formater.getFormatText()).append("\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
  
  public class KafkaPartitionConsumer implements Runnable {
    private  KafkaPartitionReader partitionReader;
    private  int batchFetch ;
    private  int fetchSize ;
    
    public KafkaPartitionConsumer(KafkaPartitionReader partitionReader, int batchFetch, int fetchSize) {
      this.partitionReader = partitionReader;
      this.batchFetch = batchFetch;
      this.fetchSize  = fetchSize;
    }
    
    @Override
    public void run() {
      try {
        doRun();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          partitionReader.commit();
          partitionReader.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    
    void doRun() throws Exception {
      int total = 0, lastCount = 0, cannotReadCount = 0;
      int fetchRetries = topicConfig.consumerConfig.consumeFetchRetries;
      while(!interrupt ) {
        List<byte[]> messages = partitionReader.fetch(fetchSize, batchFetch/*max read*/, 0 /*max wait*/,fetchRetries);
        messageCounter.count(partitionReader.getPartition(), messages.size());
        for(byte[] messagePayload : messages) {
          messageTracker.log(messageExtractor.extract(messagePayload));
        }
        total += messages.size();

        if(lastCount == total) {
          cannotReadCount++;
          Thread.sleep(1000);
        } else {
          cannotReadCount = 0;
        }
        if(cannotReadCount >= topicConfig.consumerConfig.consumeRetries) break;
        lastCount = total;
      } 
      List<byte[]> messages = 
          partitionReader.fetch(fetchSize, batchFetch/*max read*/, 0 /*max wait*/,fetchRetries);
      messageCounter.count(partitionReader.getPartition(), messages.size());
      for(byte[] messagePayload : messages) {
        messageTracker.log(messageExtractor.extract(messagePayload));
      }
      total += messages.size();
    }
  }
}