package com.neverwinterdp.kafka.tool;

import java.io.IOException;
import java.text.DecimalFormat;

import com.neverwinterdp.util.text.TabularFormater;

public class KafkaReport {
  private ProducerReport producerReport;
  private ConsumerReport consumerReport;

  public KafkaReport() {
    producerReport = new ProducerReport();
    consumerReport = new ConsumerReport();
  }

  //success = consumer.read/producer.send*100
  //
  //TODO for producer report print out reason for stopping
  //i.e. did writers stop because of reaching maxduration or max send per partitition.
  public void report(Appendable out) throws IOException {
    String[] header = { "runDur(ms)", "sent", "writes/sec", "messageSize(bytes)", "maxSend/partition", "partitions", "replication" };
    TabularFormater formater = new TabularFormater(header);
    formater.setTitle("Producer Report");
    DecimalFormat df = new DecimalFormat("0");
    double writePerSec = producerReport.messageSent / (producerReport.runDuration / 1000d);
    Object[] cells = {
        producerReport.runDuration, producerReport.messageSent, df.format(writePerSec), producerReport.messageSize,
        producerReport.maxMessagePerPartition, producerReport.partitions, producerReport.replication };
    formater.addRow(cells);
    
    out.append(formater.getFormatText());
  }

  public ProducerReport getProducerReport() {
    return producerReport;
  }

  public void setProducerReport(ProducerReport producerReport) {
    this.producerReport = producerReport;
  }

  public ConsumerReport getConsumerReport() {
    return consumerReport;
  }

  public void setConsumerReport(ConsumerReport consumerReport) {
    this.consumerReport = consumerReport;
  }

  static public class ProducerReport {
    private String topic;
    private long runDuration;
    private int messageSent;
    private int messageSize; //bytes
    private int failed; // gotten from writer
    private long maxDuration;
    private int maxMessagePerPartition;
    private long sendPeriod;
    private long sendTimeout;
    private int partitions;
    private int replication;

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public long getRunDuration() {
      return runDuration;
    }

    public void setRunDuration(long runDuration) {
      this.runDuration = runDuration;
    }

    public int getMessageSent() {
      return messageSent;
    }

    public void setMessageSent(int messageSent) {
      this.messageSent = messageSent;
    }

    public int getMessageSize() {
      return messageSize;
    }

    public void setMessageSize(int messageSize) {
      this.messageSize = messageSize;
    }

    public int getFailed() {
      return failed;
    }

    public void setFailed(int failed) {
      this.failed = failed;
    }

    public long getMaxDuration() {
      return maxDuration;
    }

    public void setMaxDuration(long maxDuration) {
      this.maxDuration = maxDuration;
    }

    public int getMaxMessagePerPartition() {
      return maxMessagePerPartition;
    }

    public void setMaxMessagePerPartition(int maxMessagePerPartition) {
      this.maxMessagePerPartition = maxMessagePerPartition;
    }

    public long getSendPeriod() {
      return sendPeriod;
    }

    public void setSendPeriod(long sendPeriod) {
      this.sendPeriod = sendPeriod;
    }

    public void setPeriod(long sendPeriod) {
      this.sendPeriod = sendPeriod;
    }

    public long getSendTimeout() {
      return sendTimeout;
    }

    public void setSendTimeout(long sendTimeout) {
      this.sendTimeout = sendTimeout;
    }

    public void setTimeout(long sendTimeout) {
      this.sendTimeout = sendTimeout;
    }

    public int getNumberOfPartition() {
      return partitions;
    }

    public void setNumberOfPartition(int numberOfPartition) {
      this.partitions = numberOfPartition;
    }

    public void setNumPartitions(int numberOfPartition) {
      this.partitions = numberOfPartition;
    }

    public int getReplication() {
      return replication;
    }

    public void setReplication(int replication) {
      this.replication = replication;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("ProducerReport [topic=");
      builder.append(topic);
      builder.append(", runDuration=");
      builder.append(runDuration);
      builder.append(", messageSent=");
      builder.append(messageSent);
      builder.append(", messageSize=");
      builder.append(messageSize);
      builder.append(", failed=");
      builder.append(failed);
      builder.append(", maxDuration=");
      builder.append(maxDuration);
      builder.append(", maxMessagePerPartition=");
      builder.append(maxMessagePerPartition);
      builder.append(", sendPeriod=");
      builder.append(sendPeriod);
      builder.append(", sendTimeout=");
      builder.append(sendTimeout);
      builder.append(", numberOfPartition=");
      builder.append(partitions);
      builder.append(", replication=");
      builder.append(replication);
      builder.append("]");
      return builder.toString();
    }

  }

  static public class ConsumerReport {
    private String topic;
    private long runDuration;
    private int messagesRead;
    private int partition;

    public String getTopic() {
      return topic;
    }

    public void setTopic(String topic) {
      this.topic = topic;
    }

    public long getRunDuration() {
      return runDuration;
    }

    public void setRunDuration(long runDuration) {
      this.runDuration = runDuration;
    }

    public int getMessagesRead() {
      return messagesRead;
    }

    public void setMessagesRead(int messagesRead) {
      this.messagesRead = messagesRead;
    }

    public int getPartition() {
      return partition;
    }

    public void setPartition(int partition) {
      this.partition = partition;
    }
  }
}
