package com.neverwinterdp.kafka.tool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;

/**
 * The goal of the tool is to test the stability of kafka with random network, broker, disk failure.
 * The tool should:
 * 1. Create a number of writer equals to the number of partition, write a message with the write period. The writer
 *    should stop when either max-message-per-partition or max-duration reach first.
 * 2. Create a number of the partitioner reader equals to the number of partition, consume the message and verify that 
 *    the number of message equals to the number of sent message in writer
 * 3. Exit the tool, when either the readers consume all the messages or the exit-wait-time expires. The exit-wait-time
 *    start when all the writers terminate.
 * @author Tuan
 */
public class KafkaTopicCheckTool implements Runnable {
  @ParametersDelegate
  private KafkaTopicConfig kafkaTopicConfig = new KafkaTopicConfig();
  
  private KafkaMessageSendTool sendTool;
  private KafkaMessageCheckTool checkTool;
  private KafkaTopicReport topicReport;
  
  private Thread deamonThread;
  private boolean running = false;

  public KafkaTopicCheckTool(String[] args, boolean showUsage) throws Exception {
    JCommander jcommander = new JCommander(this, args);
    if(showUsage) jcommander.usage();
    sendTool = new KafkaMessageSendTool(kafkaTopicConfig);
    checkTool = new KafkaMessageCheckTool(kafkaTopicConfig);
  }
  
  public KafkaTopicCheckTool(KafkaTopicConfig config) {
    kafkaTopicConfig = config;
    sendTool = new KafkaMessageSendTool(kafkaTopicConfig);
    checkTool = new KafkaMessageCheckTool(kafkaTopicConfig);
  }
  
  public KafkaTopicConfig getKafkaConfig() { return this.kafkaTopicConfig ; }

  public KafkaTopicReport getKafkaTopicReport() { return topicReport; }
  
  public KafkaMessageSendTool getKafkaMessageSendTool() { return this.sendTool; }
  
  //TODO: make sure this method work
  public void junitReport() throws Exception {
    if(kafkaTopicConfig.junitReportFile != null) {
      topicReport.junitReport(kafkaTopicConfig.junitReportFile);
    }
  }
  
  synchronized public boolean waitForTermination() throws InterruptedException {
    if (!running) return !running;
    checkTool.waitForTermination();
    Thread.sleep(500);
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
      doRun();
    } catch (Exception e) {
      e.printStackTrace();
    }
    running = false ;
  }

  private void doRun() throws Exception {
    sendTool.runAsDeamon();
    
    //Make sure that messgages are sending before start the failure simulator
    while(!sendTool.isSending()) {
      Thread.sleep(100);
    }
    Thread.sleep(500);
    
    checkTool.runAsDeamon();
    
    Thread progressReporter = new Thread() {
      public void run() {
        try {
          while(true) {
            Thread.sleep(10000);
            System.out.println("Progress: sent = " + sendTool.getSentCount() + ", consumed = " + checkTool.getMessageCounter().getTotal());
          }
        } catch (InterruptedException e) {
          System.out.println("Exit the progress reporter");
        }
      }
    };
    progressReporter.start();
    sendTool.waitForTermination();
    if(!checkTool.waitForTermination()) {
      checkTool.setInterrupt(true);
      Thread.sleep(3000);
    }
    progressReporter.interrupt();
    
    topicReport = new KafkaTopicReport();
    topicReport.setTopic(kafkaTopicConfig.topic);
    topicReport.setNumOfPartitions(kafkaTopicConfig.numberOfPartition);
    topicReport.setNumOfReplications(kafkaTopicConfig.replication);
    sendTool.populate(topicReport);
    checkTool.populate(topicReport);
  }

  static public void main(String[] args) throws Exception {
    KafkaTopicCheckTool tool = new KafkaTopicCheckTool(args, true);
    tool.run();
    //TODO: make sure this call works
    tool.junitReport();
    tool.getKafkaTopicReport().report(System.out);
  }
}
