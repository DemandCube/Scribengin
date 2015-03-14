package com.neverwinterdp.kafka.tool;

import java.io.IOException;
import java.text.DecimalFormat;

import com.neverwinterdp.util.text.TabularFormater;

public class KafkaTopicReport {
  private String topic ;
  private int    numOfPartitions;
  private int    numOfReplications;
  private int    failureSimulation;
  
  private ProducerReport producerReport;
  private ConsumerReport consumerReport;

  public KafkaTopicReport() {
    producerReport = new ProducerReport();
    consumerReport = new ConsumerReport();
  }

  public String getTopic() { return topic; }
  public void setTopic(String topic) { this.topic = topic; }
  
  public int getNumOfPartitions() { return numOfPartitions; }
  public void setNumOfPartitions(int numOfPartitions) { this.numOfPartitions = numOfPartitions; }
  
  public int getNumOfReplications() { return numOfReplications; }
  public void setNumOfReplications(int numOfReplications) { this.numOfReplications = numOfReplications; }
  
  public int getFailureSimulation() { return failureSimulation; }
  public void setFailureSimulation(int failureSimulation) { this.failureSimulation = failureSimulation; }

  public ProducerReport getProducerReport() { return producerReport; }
  public void setProducerReport(ProducerReport producerReport) { this.producerReport = producerReport; }

  public ConsumerReport getConsumerReport() { return consumerReport; }
  public void setConsumerReport(ConsumerReport consumerReport) { this.consumerReport = consumerReport; }

  public void report(Appendable out) throws IOException {
    String[] header = { 
      "Topic", "Replication", "Partitions", "F Sim", 
      "W Duration", "W Rate", "W Total", "R Duration", "R Rate", "R Total"
    };
    
    TabularFormater reportFormater = new TabularFormater(header);
    reportFormater.setTitle("Report for the topic " + topic);
    Object[] cells = {
      topic, numOfReplications, numOfPartitions, failureSimulation,
      producerReport.runDuration, producerReport.messageSent/(producerReport.runDuration/1000), producerReport.messageSent,
      consumerReport.runDuration, consumerReport.messagesRead/(consumerReport.runDuration/1000), consumerReport.messagesRead,
    };
    reportFormater.addRow(cells);
    out.append("\n");
    out.append(reportFormater.getFormatText());
  }
  
  //success = consumer.read/producer.send*100
  //
  //TODO for producer report print out reason for stopping
  //i.e. did writers stop because of reaching maxduration or max send per partitition.
  //TODO aggregate both reports in one matrix
  public void oldReport(Appendable out) throws IOException {
    String[] producerHeader = { 
      "Topic", "runDur(ms)", "sent", "writes/sec", "messageSize(bytes)", "partitions", "replication" 
    };
    
    TabularFormater producerFormater = new TabularFormater(producerHeader);
    producerFormater.setTitle("Producer Report");
    DecimalFormat df = new DecimalFormat("0");
    double writePerSec = producerReport.messageSent / (producerReport.runDuration / 1000d);
    Object[] producer = {
        topic, producerReport.runDuration, producerReport.messageSent, df.format(writePerSec),
        producerReport.messageSize, numOfPartitions, numOfReplications };
    producerFormater.addRow(producer);

    out.append(producerFormater.getFormatText());

    String[] consumerHeader = { "topic", "runDur(ms)", "consumed", "consumed/sec", "partitions" };
    TabularFormater consumerFormater = new TabularFormater(consumerHeader);
    consumerFormater.setTitle("Consumer Report");
    double readPerSec = consumerReport.messagesRead / (consumerReport.runDuration / 1000d);
    Object[] consumer = {
      topic, consumerReport.runDuration, consumerReport.messagesRead, df.format(readPerSec), numOfPartitions,
    };
    consumerFormater.addRow(consumer);
    out.append("\n");
    out.append(consumerFormater.getFormatText());
  }

  static public class ProducerReport {
    private long runDuration;
    private int messageSent;
    private int messageSize; //bytes
    private int failed; // gotten from writer

    public long getRunDuration() {  return runDuration; }

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

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("ProducerReport [runDuration=");
      builder.append(runDuration);
      builder.append(", messageSent=");
      builder.append(messageSent);
      builder.append(", messageSize=");
      builder.append(messageSize);
      builder.append(", failed=");
      builder.append(failed);
      builder.append("]");
      return builder.toString();
    }
  }

  static public class ConsumerReport {
    private long   runDuration;
    private int    messagesRead;

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

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("ConsumerReport [");
      builder.append("runDuration=").append(runDuration).append(",");
      builder.append("messagesRead=").append(messagesRead);
      builder.append("]");
      return builder.toString();
    }
  }
}
