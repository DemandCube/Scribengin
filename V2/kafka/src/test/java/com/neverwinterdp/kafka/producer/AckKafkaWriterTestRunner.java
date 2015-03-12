package com.neverwinterdp.kafka.producer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.google.common.base.Stopwatch;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.kafka.KafkaCluster;
import com.neverwinterdp.util.FileUtil;
import com.neverwinterdp.util.text.TabularFormater;

/**
 * @author Tuan
 */
public class AckKafkaWriterTestRunner {
  static String NAME = "test";

  private KafkaCluster cluster;
  private Report report;

  private AckKafkaWriterTestRunnerConfig config;

  public AckKafkaWriterTestRunner(AckKafkaWriterTestRunnerConfig config) {
    this.config = config;
  }

  public Report getReport() {
    return this.report;
  }

  public void setUp() throws Exception {
    FileUtil.removeIfExist("./build/kafka", false);
    report = new Report();
    cluster = new KafkaCluster("./build/kafka", 1, config.getNumKafkaBrokers());
    cluster.setReplication(config.getNumOfReplications());
    cluster.setNumOfPartition(config.getNumOfPartitions());
    cluster.start();
    Thread.sleep(2000);

  }

  public void tearDown() throws Exception {
     cluster.shutdown();
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    for (Thread sel : threadSet) {
      System.err.println("Thread: " + sel.getName());
    }
  }

  public void run() throws Exception {
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

    KafkaTool kafkaTool = new KafkaTool(config.getTopic(), cluster.getZKConnect());
    kafkaTool.connect();
    kafkaTool.createTopic(config.getTopic(), config.getNumOfReplications(), config.getNumOfPartitions());
    kafkaTool.close();

    KafkaMessageSendTool sendTool = new KafkaMessageSendTool(config.getTopic(), config.getMaxNumOfMessages(),
        config.getMessageSize());
    new Thread(sendTool).start();
    Thread.sleep(100);

    KafkapartitionLeaderKiller leaderKiller = new KafkapartitionLeaderKiller(config.getTopic(), 0, 3000);
    new Thread(leaderKiller).start();

    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool(cluster.getZKConnect(), config.getTopic(),
        config.getMaxNumOfMessages());
    checkTool.setFetchSize(config.getMessageSize() * 125);
    checkTool.runAsDeamon();

    sendTool.waitTermination(300000); // send for max 5 mins
    leaderKiller.exit();
    //make sure that no server shutdown when run the check tool
    //The check tool is not designed to read from the broken server env
    leaderKiller.waitForTermination(30000);
    System.out.println("Finished sending, waiting for check tool..............");
    checkTool.waitForTermination(300000);
  
    report.setSent(sendTool.getNumOfSentMessages());
    report.setFailedAck(sendTool.getNumOfFailedAck());
    report.setConsumed(checkTool.getMessageCounter().getTotal());
    report.setKafkaBrokerRestartCount(leaderKiller.getFaillureCount());
    report.setMessageSize(sendTool.messageSize);
    report.setPartitions(config.getNumOfPartitions());
    report.setReplicationFactor(config.getNumOfReplications());
    report.setBrokerCount(config.getNumKafkaBrokers());
    report.setWriteDuration(sendTool.stopwatch);
    report.setReadDuration(checkTool.getReadDuration());
    checkTool.getMessageCounter().print(System.out, "Topic: " + config.getTopic());
    
  }

  class KafkaMessageSendTool implements Runnable {
    private String topic;
    private int maxNumOfMessages = 10000;
    private int messageSize = 1024;
    private long waitBeforeClose = 10000;
    private int numOfSentMessages = 0;
    private int numOfFailedAck = 0;
    private boolean exit = false;
    private Stopwatch stopwatch = Stopwatch.createUnstarted();

    public KafkaMessageSendTool(String topic, int maxNumOfMessages, int messageSize) {
      this.topic = topic;
      this.maxNumOfMessages = maxNumOfMessages;
      this.messageSize = messageSize;
    }

    public int getNumOfSentMessages() {
      return this.numOfSentMessages;
    }

    public int getNumOfFailedAck() {
      return this.numOfFailedAck;
    }

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
      stopwatch.start();
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
      FailedAckReportCallback failedAckReportCallback = new FailedAckReportCallback();
      while (!exit && numOfSentMessages < maxNumOfMessages) {
        //Use this send to print out more detail about the message lost
        byte[] key = ("key-" + numOfSentMessages).getBytes();
        byte[] message = new byte[messageSize];
        writer.send(topic, key, message, failedAckReportCallback, 10000);
        numOfSentMessages++;
      }
      writer.waitAndClose(waitBeforeClose);
      numOfFailedAck = failedAckReportCallback.getCount();
      stopwatch.stop();
    }

    synchronized public void notifyTermination() {
      notify();
    }

    synchronized public void waitTermination(long timeout) throws InterruptedException {
      wait(timeout);
      exit = true;
    }
  }

  class FailedAckReportCallback implements Callback {
    private int count;

    public int getCount() {
      return count;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null)
        count++;
    }
  }

  class KafkapartitionLeaderKiller implements Runnable {
    private String topic;
    private int partition;
    private long sleepBeforeRestart = 500;
    private int failureCount;
    private boolean exit = false;

    KafkapartitionLeaderKiller(String topic, int partition, long sleepBeforeRestart) {
      this.topic = topic;
      this.partition = partition;
      this.sleepBeforeRestart = sleepBeforeRestart;
    }

    public int getFaillureCount() {
      return this.failureCount;
    }

    public void exit() {
      exit = true;
    }

    public void run() {
      try {
        while (!exit) {
          KafkaTool kafkaTool = new KafkaTool(topic, cluster.getZKConnect());
          kafkaTool.connect();
          TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topic);
          PartitionMetadata partitionMeta = findPartition(topicMeta, partition);
          Broker partitionLeader = partitionMeta.leader();
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
      synchronized (this) {
        notify();
      }
    }

    PartitionMetadata findPartition(TopicMetadata topicMetadata, int partion) {
      for (PartitionMetadata sel : topicMetadata.partitionsMetadata()) {
        if (sel.partitionId() == partition)
          return sel;
      }
      throw new RuntimeException("Cannot find the partition " + partition);
    }

    synchronized public void waitForTermination(long timeout) throws InterruptedException {
      wait(timeout);
    }
  }

  static public class Report {
    private int sent;
    private int consumed;
    private int failedAck;
    private int restartCount;
    private int messageSize;
    private int partitions;
    private int replicationFactor;
    private int brokerCount;
    private Stopwatch writeDuration;
    private Stopwatch readDuration;
    private Stopwatch runDuration;

    public int getSent() {
      return sent;
    }

    public void setSent(int sent) {
      this.sent = sent;
    }

    public int getConsumed() {
      return consumed;
    }

    public void setConsumed(int consumed) {
      this.consumed = consumed;
    }

    public int getFailedAck() {
      return failedAck;
    }

    public void setFailedAck(int failedAck) {
      this.failedAck = failedAck;
    }

    public int getKafkaBrokerRestartCount() {
      return restartCount;
    }

    public void setKafkaBrokerRestartCount(int kafkaBrokerRestartCount) {
      this.restartCount = kafkaBrokerRestartCount;
    }

    public int getMessageSize() {
      return messageSize;
    }

    public void setMessageSize(int messageSize) {
      this.messageSize = messageSize;
    }

    public int getPartitions() {
      return partitions;
    }

    public void setPartitions(int partitions) {
      this.partitions = partitions;
    }

    public int getReplicationFactor() {
      return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
      this.replicationFactor = replicationFactor;
    }

    public Stopwatch getWriteDuration() {
      return writeDuration;
    }

    public void setWriteDuration(Stopwatch writeDuration) {
      this.writeDuration = writeDuration;
    }

    public int getBrokerCount() {
      return brokerCount;
    }

    public void setBrokerCount(int brokerCount) {
      this.brokerCount = brokerCount;
    }

    public Stopwatch getReadDuration() {
      return readDuration;
    }

    public void setReadDuration(Stopwatch readDuration) {
      this.readDuration = readDuration;
    }

    public Stopwatch getRunDuration() {
      return runDuration;
    }

    public void setRunDuration(Stopwatch runDuration) {
      this.runDuration = runDuration;
    }

    public void print(Appendable out, String title) {
      TabularFormater formater = new TabularFormater("Sent", "Failed Ack", "Consumed", "Broker restarts",
          "message size(bytes)", "run Duration");
      if (title != null && title.isEmpty())
        formater.setTitle(title);

      formater.setIndent("  ");
      formater.addRow(sent, failedAck, consumed, restartCount, messageSize, writeDuration);

      try {
        out.append(formater.getFormatText()).append("\n");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }
}