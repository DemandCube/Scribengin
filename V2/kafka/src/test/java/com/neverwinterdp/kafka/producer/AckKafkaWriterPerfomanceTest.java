package com.neverwinterdp.kafka.producer;

import java.text.DecimalFormat;

import org.junit.Test;

//TODO success rate 2 dp

import com.neverwinterdp.kafka.producer.AckKafkaWriterTestRunner.Report;
import com.neverwinterdp.util.text.TabularFormater;

public class AckKafkaWriterPerfomanceTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  @Test
  public void testRunner() throws Exception {
    String[][] args = {
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "1","--num-replication", "2","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "30000", "--num-partition", "1","--num-replication", "2","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "30000", "--num-partition", "10","--num-replication", "2","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "20","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "40","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "50","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "75","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "100000", "--num-partition", "1","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "100000", "--num-partition", "2","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "30000", "--num-partition", "3","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "2048", "--max-num-message", "30000", "--num-partition", "2", "--num-replication", "2","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "2048", "--max-num-message", "50000", "--num-partition", "5", "--num-replication", "2","--num-kafka-brokers", "2"},
        { "--topic", "hello", "--message-size", "4096", "--max-num-message", "30000", "--num-partition", "10","--num-replication", "2","--num-kafka-brokers", "3" },
        { "--topic", "hello", "--message-size", "4096", "--max-num-message", "100000", "--num-partition", "10","--num-replication", "3","--num-kafka-brokers", "3" },
        { "--topic", "hello", "--message-size", "512000", "--max-num-message", "10000", "--num-partition", "1","--num-replication", "3","--num-kafka-brokers", "3" }       
    };

    AckKafkaWriterTestRunner.Report[] reports = new AckKafkaWriterTestRunner.Report[args.length];
    for (int i = 0; i < args.length; i++) {
      AckKafkaWriterTestRunnerConfig config = new AckKafkaWriterTestRunnerConfig(args[i]);
      AckKafkaWriterTestRunner runner = new AckKafkaWriterTestRunner(config);
      runner.setUp();
      runner.run();
      reports[i] = runner.getReport();
      runner.tearDown();
    }
    formatReports("Perfomance Tests", reports);
 }

  private void formatReports(String title, Report[] reports) {
    String[] header = {"Sent", "failed", "consumed","success %", "partitions","replication","brokers", "restarts", "bytes","writeDuration", "readDuration"} ;
    TabularFormater formater = new TabularFormater(header);
    formater.setTitle(title);
    DecimalFormat df = new DecimalFormat("0.00");
    double successRate=0.0d;
    for (Report report : reports) {
      successRate=  report.getConsumed() * 100f / report.getSent();
      Object[] cells = {
          report.getSent(), report.getFailedAck(), report.getConsumed(),df.format(successRate) , report.getPartitions(),
          report.getReplicationFactor(),report.getBrokerCount(), report.getKafkaBrokerRestartCount(), report.getMessageSize(),report.getWriteDuration(), report.getReadDuration()
      };
      formater.addRow(cells);
    }
    System.out.println(formater.getFormatText());
  }
}
