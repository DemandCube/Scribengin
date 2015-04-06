package com.neverwinterdp.kafka.tool;

import java.io.File;
import java.io.IOException;

import org.tap4j.model.TestResult;
import org.tap4j.model.TestSet;
import org.tap4j.producer.TapProducer;
import org.tap4j.producer.TapProducerFactory;
import org.tap4j.util.StatusValues;

import com.neverwinterdp.util.text.TabularFormater;

public class KafkaTopicReport {
  private String topic;
  private int numOfPartitions;
  private int numOfReplications;
  private int failureSimulation;

  private ProducerReport producerReport;
  private ConsumerReport consumerReport;

  public KafkaTopicReport() {
    producerReport = new ProducerReport();
    consumerReport = new ConsumerReport();
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public int getNumOfPartitions() {
    return numOfPartitions;
  }

  public void setNumOfPartitions(int numOfPartitions) {
    this.numOfPartitions = numOfPartitions;
  }

  public int getNumOfReplications() {
    return numOfReplications;
  }

  public void setNumOfReplicas(int numOfReplications) {
    this.numOfReplications = numOfReplications;
  }

  public int getFailureSimulation() {
    return failureSimulation;
  }

  public void setFailureSimulation(int failureSimulation) {
    this.failureSimulation = failureSimulation;
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

  public void report(Appendable out) throws IOException {
    report(out, this);
  }

  static public void report(Appendable out, KafkaTopicReport... report) throws IOException {
    String[] header = {
        "Topic", "Replication", "Partitions", "F Sim",
        "Writer", "W Duration", "W Rate", "W Total", "W Failed",
        "R Duration", "R Rate", "R Total"
    };

    TabularFormater reportFormater = new TabularFormater(header);
    reportFormater.setTitle("Topic Report ");
    for (KafkaTopicReport sel : report) {
      long messageSentRate = 0;
      if (sel.producerReport.messageSent > 0) {
        messageSentRate = sel.producerReport.messageSent / (sel.producerReport.runDuration / 1000);
      }
      long messageReadRate = 0;
      if (sel.consumerReport.messagesRead > 0) {
        messageReadRate = sel.consumerReport.messagesRead / (sel.consumerReport.runDuration / 1000);
      }
      Object[] cells = {
          sel.topic, sel.numOfReplications, sel.numOfPartitions, sel.failureSimulation,
          sel.producerReport.writer, sel.producerReport.runDuration, messageSentRate, sel.producerReport.messageSent,
          sel.producerReport.messagesRetried,
          sel.consumerReport.runDuration, messageReadRate, sel.consumerReport.messagesRead,
      };
      reportFormater.addRow(cells);
    }
    out.append("\n");
    out.append(reportFormater.getFormatText());

  }

  public void junitReport(String fileName) throws Exception {
    TestSet testSet = new TestSet();
    int testNum = 0;

    testSet.addTestResult(newTestResult(++testNum,
        "Messages sent: " + producerReport.messageSent,
        producerReport.messageSent > 0));

    testSet.addTestResult(newTestResult(++testNum,
        "Messages sent: " + producerReport.messageSent + " --- Messages Consumed: " + consumerReport.messagesRead,
        consumerReport.messagesRead >= producerReport.messageSent));

    testSet.addTestResult(newTestResult(++testNum,
        "Messages retried: " + producerReport.messagesRetried,
        true));

    testSet.addTestResult(newTestResult(++testNum,
        "Producer run duration: " + producerReport.runDuration + " --- Consumer run duration: "
            + consumerReport.runDuration,
        producerReport.runDuration > 0 && consumerReport.runDuration > 0));

    testSet.addTestResult(newTestResult(++testNum,
        "Producer Message Size: " + producerReport.messageSize,
        producerReport.messageSize > 0));

    File file = new File(fileName);
    file.getParentFile().mkdirs();
    file.createNewFile();

    TapProducer tapProducer = TapProducerFactory.makeTapJunitProducer(getClass().getSimpleName());
    tapProducer.dump(testSet, file);
  }

  private TestResult newTestResult(int testNum, String desc, boolean passed) {
    TestResult tr = null;
    if (passed) {
      tr = new TestResult(StatusValues.OK, testNum);
    } else {
      tr = new TestResult(StatusValues.NOT_OK, testNum);
    }
    tr.setDescription(desc);
    return tr;
  }

  static public class ProducerReport {
    private String writer;
    private long runDuration;
    private int messageSent;
    private int messageSize; //bytes
    private long messagesRetried; // messages that needed to be retried

    public String getWriter() {
      return writer;
    }

    public void setWriter(String writer) {
      this.writer = writer;
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

    public long getMessageRetried() {
      return messagesRetried;
    }

    public void setMessageRetried(long retried) {
      this.messagesRetried = retried;
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
      builder.append(", messagesRetried=");
      builder.append(messagesRetried);
      builder.append("]");
      return builder.toString();
    }
  }

  static public class ConsumerReport {
    private long runDuration;
    private int messagesRead;

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
