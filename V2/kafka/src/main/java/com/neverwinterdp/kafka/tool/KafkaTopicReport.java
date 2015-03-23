package com.neverwinterdp.kafka.tool;

import java.io.IOException;

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
    report(out, this);
  }
  
  static public void report(Appendable out, KafkaTopicReport ... report) throws IOException {
    String[] header = { 
        "Topic", "Replication", "Partitions", "F Sim", 
        "Writer","W Duration", "W Rate", "W Total", "R Duration", "R Rate", "R Total"
      };
      
      TabularFormater reportFormater = new TabularFormater(header);
      reportFormater.setTitle("Topic Report ");
      for(KafkaTopicReport  sel : report) {
        long messageSentRate = 0 ;
        if(sel.producerReport.messageSent > 0) {
          messageSentRate = sel.producerReport.messageSent/(sel.producerReport.runDuration/1000);
        }
        long messageReadRate = 0;
        if(sel.consumerReport.messagesRead > 0) {
          messageReadRate = sel.consumerReport.messagesRead/(sel.consumerReport.runDuration/1000);
        }
        Object[] cells = {
          sel.topic, sel.numOfReplications, sel.numOfPartitions, sel.failureSimulation,
          sel.producerReport.writer, sel.producerReport.runDuration, messageSentRate, sel.producerReport.messageSent,
          sel.consumerReport.runDuration, messageReadRate, sel.consumerReport.messagesRead,
        };
        reportFormater.addRow(cells);
      }
      out.append("\n");
      out.append(reportFormater.getFormatText());
  }
  
  static public class ProducerReport {
    private String writer;
    private long   runDuration;
    private int    messageSent;
    private int    messageSize; //bytes
    private int    failed; // gotten from writer

    public String getWriter() { return writer; }
    public void setWriter(String writer) { this.writer = writer; }
    
    public long getRunDuration() {  return runDuration; }
    public void setRunDuration(long runDuration) { this.runDuration = runDuration; }

    public int getMessageSent() { return messageSent; }
    public void setMessageSent(int messageSent) { this.messageSent = messageSent; }

    public int getMessageSize() { return messageSize; }
    public void setMessageSize(int messageSize) { this.messageSize = messageSize; }

    public int getFailed() { return failed; }
    public void setFailed(int failed) { this.failed = failed; }

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
