package com.neverwinterdp.kafka.tool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.neverwinterdp.kafka.consumer.KafkaPartitionReader;
import com.neverwinterdp.util.text.TabularFormater;

public class KafkaMessageCheckTool implements Runnable {
  private String name = "KafkaMessageCheckTool";
  private String zkConnect;
  private String topic;
  private int    expectNumberOfMessage;
  private int    fetchSize = 100 * 1024;
  private MessageCounter messageCounter ;
  private boolean interrupt = false ;
  private Thread deamonThread ;
  
  public KafkaMessageCheckTool(String zkConnect, String topic, int expect) {
    this.zkConnect = zkConnect;
    this.topic = topic;
    this.expectNumberOfMessage = expect;
  }
  
  public void setFetchSize(int fetchSize) { this.fetchSize = fetchSize; }
  
  public MessageCounter getMessageCounter() { return messageCounter; }
  
  public void setInterrupt(boolean b) { this.interrupt = b ; }
  
  synchronized public void waitForTermination(long maxWaitTime) throws InterruptedException {
    wait(maxWaitTime);
  }
  
  synchronized void notifyTermination() {
    notifyAll() ;
  }
  
  public void run() {
    try {
      check() ;
    } catch (Exception e) {
      e.printStackTrace();
    }
    notifyTermination();
  }
  
  public void runAsDeamon() {
    if(deamonThread != null && deamonThread.isAlive()) {
      throw new RuntimeException("Deamon thread is already started") ;
    }
    deamonThread = new Thread(this);
    deamonThread.start();
  }
  
  public void check() throws Exception {
    KafkaTool kafkaTool = new KafkaTool(name, zkConnect);
    kafkaTool.connect();
    TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topic);
    List<PartitionMetadata> partitionMetas = topicMeta.partitionsMetadata();
    kafkaTool.close();
    
    KafkaPartitionReader[] partitionReader = new KafkaPartitionReader[partitionMetas.size()];
    for(int i = 0; i < partitionReader.length; i++) {
      partitionReader[i] = new KafkaPartitionReader(name, topic, partitionMetas.get(i));
    }
    
    messageCounter = new MessageCounter();
    interrupt = false;
    while(messageCounter.getTotal() < expectNumberOfMessage && !interrupt) {
      for(int k = 0; k < partitionReader.length; k++) {
        List<byte[]> messages = partitionReader[k].fetch(fetchSize, 100/*max read*/, 1000 /*max wait*/);
        messageCounter.count(partitionReader[k].getPartition(), messages.size());
      }
    }
    
    for(int k = 0; k < partitionReader.length; k++) {
      partitionReader[k].commit();
      partitionReader[k].close();
    }
  }
  
  static public class MessageCounter {
    private Map<Integer, Integer> counters = new HashMap<Integer, Integer>();
    private int totalMessages;
 
    public int getTotal() { return totalMessages; }
    
    public int getPartitionCount(int partition) { return counters.get(partition); }
    
    public void count(int partition, int readMessage) {
      Integer current = counters.get(partition);
      if(current == null) {
        counters.put(partition, readMessage);
      } else {
        counters.put(partition, current.intValue() + readMessage);
      }
      totalMessages += readMessage;
    }
    
    public void print(Appendable out, String title) {
      TabularFormater formater = new TabularFormater("Partition", "Read");
      formater.setTitle(title + "(" + totalMessages  + ")");
      
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