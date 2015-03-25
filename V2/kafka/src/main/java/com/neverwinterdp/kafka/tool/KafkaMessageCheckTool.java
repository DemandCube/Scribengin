package com.neverwinterdp.kafka.tool;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.tap4j.model.TestResult;
import org.tap4j.model.TestSet;
import org.tap4j.producer.TapProducer;
import org.tap4j.producer.TapProducerFactory;
import org.tap4j.util.StatusValues;

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
  
  //TODO: replace by the KafkaTopicReport.ConsumerReport
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
    System.out.println("KafkaMessageCheckTool: Start running kafka message check tool.  Expecting "+Integer.toString(topicConfig.consumerConfig.consumeMax)+" messages");
    readDuration.start();
    KafkaTool kafkaTool = new KafkaTool(NAME, topicConfig.zkConnect);
    kafkaTool.connect();
    
    TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topicConfig.topic, 3);
    List<PartitionMetadata> partitionMetas = topicMeta.partitionsMetadata();
    this.numOfPartitions = partitionMetas.size();
    kafkaTool.close();
    
    KafkaPartitionReader[] partitionReader = new KafkaPartitionReader[partitionMetas.size()];
    for (int i = 0; i < partitionReader.length; i++) {
      partitionReader[i] = 
        new KafkaPartitionReader(NAME, topicConfig.zkConnect, topicConfig.topic, partitionMetas.get(i));
    }
    interrupt = false;
    int lastCount = 0, cannotReadCount = 0;
    int fetchSize = topicConfig.consumerConfig.consumeBatchFetch * (topicConfig.producerConfig.messageSize + 100) ;
    while (messageCounter.getTotal() < topicConfig.consumerConfig.consumeMax && !interrupt) {
      for (int k = 0; k < partitionReader.length; k++) {
        List<byte[]> messages = partitionReader[k].fetch(fetchSize, 1000/*max read*/, 1000 /*max wait*/);
        messageCounter.count(partitionReader[k].getPartition(), messages.size());
        for(byte[] messagePayload : messages) {
          messageTracker.log(messageExtractor.extract(messagePayload));
        }
      }
      if(lastCount == messageCounter.getTotal()) {
        cannotReadCount++;
      } else {
        cannotReadCount = 0;
      }
      if(cannotReadCount >= 5) interrupt = true;
      lastCount = messageCounter.getTotal();
    }
    //Run the last fetch to find the duplicated messages if there are some
    for (int k = 0; k < partitionReader.length; k++) {
      List<byte[]> messages = partitionReader[k].fetch(fetchSize, 100/*max read*/, 1000 /*max wait*/);
      messageCounter.count(partitionReader[k].getPartition(), messages.size());
      for(byte[] messagePayload : messages) {
        messageTracker.log(messageExtractor.extract(messagePayload));
      }
    }

    for (int k = 0; k < partitionReader.length; k++) {
      partitionReader[k].commit();
      partitionReader[k].close();
    }
    
    System.out.println("Read count: " + messageCounter.getTotal() +"(Stop)") ;
    messageTracker.optimize();
    readDuration.stop();
    
    //TODO: Move this code to KafkaTopicReport junitReport()
    TapProducer tapProducer = null;
    TestSet testSet =null;
    if(topicConfig.consumerConfig.tapEnabled){
      tapProducer = TapProducerFactory.makeTapJunitProducer(topicConfig.consumerConfig.tapFile);
      testSet = new TestSet();
      int testNum=0;
      
      //Create test result for total messages read
      TestResult t = null;
      if(messageCounter.getTotal() >= topicConfig.consumerConfig.consumeMax){
        t = new TestResult( StatusValues.OK, ++testNum );
      }
      else{
        t = new TestResult( StatusValues.NOT_OK, ++testNum );
      }
      t.setDescription("Total Expected Messages: "+ Integer.toString(topicConfig.consumerConfig.consumeMax)+
          " Total Messages Read: "+ Integer.toString(messageCounter.getTotal()));
      testSet.addTestResult( t );
      
      tapProducer.dump(testSet, new File(topicConfig.consumerConfig.tapFile));
    }
  }

  public KafkaTopicReport getReport() {
    KafkaTopicReport report = new KafkaTopicReport() ;
    report.setTopic(topicConfig.topic);
    report.setNumOfPartitions(numOfPartitions);
    report.setNumOfReplications(topicConfig.replication);
    populate(report);
    return report ;
  }
  
  
  public void populate(KafkaTopicReport report) {
    ConsumerReport consumerReport = report.getConsumerReport();
    consumerReport.setMessagesRead(messageCounter.totalMessages);
    consumerReport.setRunDuration(readDuration.elapsed(TimeUnit.MILLISECONDS));
  }

  static public class MessageCounter {
    private Map<Integer, Integer> counters = new HashMap<Integer, Integer>();
    //TODO use atomic integer for thread safety
    private int totalMessages;

    public int getTotal() {
      return totalMessages;
    }
    
    public Map<Integer, Integer> getCounter(){
      return counters;
    }

    public int getPartitionCount(int partition) {
      return counters.get(partition);
    }

    public void count(int partition, int readMessage) {
      Integer current = counters.get(partition);
      if (current == null) {
        counters.put(partition, readMessage);
      } else {
        counters.put(partition, current.intValue() + readMessage);
      }
      totalMessages += readMessage;
    }

    public void print(Appendable out, String title) {
      TabularFormater formater = new TabularFormater("Partition", "Read");
      formater.setTitle(title + "(" + totalMessages + ")");

      formater.setIndent("  ");
      for (Map.Entry<Integer, Integer> entry : counters.entrySet()) {
        formater.addRow(entry.getKey(), entry.getValue());
      }

      try {
        out.append(formater.getFormatText()).append("\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}