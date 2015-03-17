package com.neverwinterdp.kafka.tool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.beust.jcommander.ParametersDelegate;
import com.google.common.base.Stopwatch;
import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.kafka.tool.KafkaTopicReport.ConsumerReport;
import com.neverwinterdp.util.text.TabularFormater;

public class KafkaMessageCheckTool implements Runnable {
  static private String NAME = "KafkaMessageCheckTool";

  @ParametersDelegate
  private KafkaTopicConfig topicConfig = new KafkaTopicConfig();
  
  private int maxPartitionCountRetries = 20;
  private int expectNumberOfMessage;
  private int fetchSize = 500 * 1024;
  private MessageCounter messageCounter = new MessageCounter();
  private boolean interrupt = false;
  private Thread deamonThread;
  private Stopwatch readDuration = Stopwatch.createUnstarted();
  private boolean running = false;

  public KafkaMessageCheckTool() {
  }

  public KafkaMessageCheckTool(String zkConnect, String topic, int expect) {
    topicConfig.zkConnect = zkConnect;
    topicConfig.topic = topic;
    expectNumberOfMessage = expect;
  }

  public KafkaMessageCheckTool(KafkaTopicConfig topicConfig) {
    this.topicConfig = topicConfig;
    expectNumberOfMessage = topicConfig.consumerConfig.consumeMax;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  //TODO: replace by the KafkaTopicReport.ConsumerReport
  public MessageCounter getMessageCounter() {
    return messageCounter;
  }

  public Stopwatch getReadDuration() {
    return readDuration;
  }

  public void setInterrupt(boolean b) {
    this.interrupt = b;
  }

  public void setExpectNumberOfMessage(int num) {
    expectNumberOfMessage = num;
  }

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
    System.out.println("KafkaMessageCheckTool: Start running kafka message check tool.  Expecting "+Integer.toString(this.expectNumberOfMessage)+ " messages.");
    readDuration.start();
    KafkaTool kafkaTool = new KafkaTool(NAME, topicConfig.zkConnect);
    kafkaTool.connect();
    
    System.out.println("topic: "+topicConfig.topic);
    TopicMetadata topicMeta;
    List<PartitionMetadata> partitionMetas;
    int tries = 0;
    
    do{
      topicMeta = kafkaTool.findTopicMetadata(topicConfig.topic);
      partitionMetas = topicMeta.partitionsMetadata();
      tries++;
      if(partitionMetas.size() < 1){
        Thread.sleep(100*tries);
      }
    }while(partitionMetas.size() < 1 && tries < maxPartitionCountRetries);
    kafkaTool.close();
    
    System.out.println("partitions: "+Integer.toString(partitionMetas.size()));
    
    KafkaPartitionReader[] partitionReader = new KafkaPartitionReader[partitionMetas.size()];
    for (int i = 0; i < partitionReader.length; i++) {
      partitionReader[i] = 
        new KafkaPartitionReader(NAME, topicConfig.zkConnect, topicConfig.topic, partitionMetas.get(i));
    }
    interrupt = false;
    int lastCount = 0, cannotReadCount = 0;
    /**
     * 
     * ------------------------------------------------------------------------
Submit the dataflow hello-kafka-dataflow
------------------------------------------------------------------------
Wait time to finish: 60000msKafkaMessageCheckTool: Start running kafka message check tool.  Expecting 100 messages.

partitions: 0
INTERRUPT: false
Expecting 100 messages.
TOTAL: 0
Partition length: 0
last count: 0
Partition length: 0
last count: 0
Partition length: 0
last count: 0
Partition length: 0
last count: 0
Partition length: 0
last count: 0
Read count: 0(Stop)
     */
    System.out.print("INTERRUPT: ");
    System.out.println(interrupt);
    System.out.println("Expecting "+Integer.toString(this.expectNumberOfMessage)+ " messages.");
    System.out.println("TOTAL: "+Integer.toString(messageCounter.getTotal()));
    System.out.println("Partition length: "+Integer.toString(partitionReader.length));
    
    while (messageCounter.getTotal() < expectNumberOfMessage && !interrupt) {
      for (int k = 0; k < partitionReader.length; k++) {
        List<byte[]> messages;
        try{
          messages = partitionReader[k].fetch(fetchSize, 100/*max read*/, 1000 /*max wait*/);
        } catch(Exception e){
          messageCounter.count(partitionReader[k].getPartition(), 0);
          continue;
        }
        messageCounter.count(partitionReader[k].getPartition(), messages.size());
        System.out.println("Just read: "+Integer.toString(messageCounter.getTotal()));
      }
      if (lastCount == messageCounter.getTotal()) {
        cannotReadCount++;
      } else {
        cannotReadCount = 0;
      }
      if(cannotReadCount >= 5) interrupt = true;
      lastCount = messageCounter.getTotal();
      System.out.println("last count: "+Integer.toString(lastCount));
      System.out.println("Cannot read count: "+Integer.toString(cannotReadCount));
      System.out.print("interrupt: ");
      System.out.println(interrupt);
    }
    //Run the last fetch to find the duplicated messages if there are some
    for (int k = 0; k < partitionReader.length; k++) {
      List<byte[]> messages = partitionReader[k].fetch(fetchSize, 100/*max read*/, 1000 /*max wait*/);
      messageCounter.count(partitionReader[k].getPartition(), messages.size());
    }

    for (int k = 0; k < partitionReader.length; k++) {
      partitionReader[k].commit();
      partitionReader[k].close();
    }
    System.out.println("Read count: " + messageCounter.getTotal() +"(Stop)") ;
    readDuration.stop();
  }

  public void report(KafkaTopicReport report) {
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