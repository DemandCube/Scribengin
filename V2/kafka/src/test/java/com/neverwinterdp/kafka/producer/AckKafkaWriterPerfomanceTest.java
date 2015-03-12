package com.neverwinterdp.kafka.producer;

import java.text.DecimalFormat;

import org.junit.Test;

//TODO success rate 2 dp
//TODO add total tile for each run

import com.google.common.base.Stopwatch;
import com.neverwinterdp.kafka.producer.AckKafkaWriterTestRunner.Report;
import com.neverwinterdp.util.text.TabularFormater;

public class AckKafkaWriterPerfomanceTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  private Stopwatch totalRunDuration = Stopwatch.createUnstarted();
  private Stopwatch runDuration = Stopwatch.createUnstarted();

  @Test
  public void testRunner() throws Exception {
    String[][] args = {
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "1","--num-replication", "2","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "1","--num-replication", "2","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "10","--num-replication", "2","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "20","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "40","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "50","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "75","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "100000", "--num-partition", "1", "--num-replication", "3", "--num-kafka-brokers", "3" },
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "100000", "--num-partition", "2","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "10000", "--num-partition", "3","--num-replication", "3","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "2048", "--max-num-message", "10000", "--num-partition", "2", "--num-replication", "2","--num-kafka-brokers", "3"},
        { "--topic", "hello", "--message-size", "2048", "--max-num-message", "50000", "--num-partition", "5", "--num-replication", "2","--num-kafka-brokers", "2"},
        { "--topic", "hello", "--message-size", "4096", "--max-num-message", "10000", "--num-partition", "10","--num-replication", "2","--num-kafka-brokers", "3" },
        { "--topic", "hello", "--message-size", "4096", "--max-num-message", "100000", "--num-partition", "10","--num-replication", "3","--num-kafka-brokers", "3" },
        { "--topic", "hello", "--message-size", "512000", "--max-num-message", "10000", "--num-partition", "1","--num-replication", "3","--num-kafka-brokers", "3" }       
    };
    totalRunDuration.start();
    AckKafkaWriterTestRunner.Report[] reports = new AckKafkaWriterTestRunner.Report[args.length];
    for (int i = 0; i < args.length; i++) {
      AckKafkaWriterTestRunnerConfig config = new AckKafkaWriterTestRunnerConfig(args[i]);
      AckKafkaWriterTestRunner runner = new AckKafkaWriterTestRunner(config);
      runner.setUp();
      runDuration.start();
      runner.run();
      runDuration.stop();
      reports[i] = runner.getReport();
      reports[i].setRunDuration(runDuration);
      runner.tearDown();
    }
    totalRunDuration.stop();
    formatReports("Perfomance Tests", reports);
  }

  private void formatReports(String title, Report[] reports) {
    String[] header = { "Sent", "failed", "consumed", "success%", "partitions", "replication", "brokers", "restarts","bytes", "writeDur", "readDur", "runDur" };
    TabularFormater formater = new TabularFormater(header);
    formater.setTitle(title);
    DecimalFormat df = new DecimalFormat("0.00");
    double successRate = 0.0d;
    for (Report report : reports) {
      successRate = report.getConsumed() * 100f / report.getSent();
      Object[] cells = {
          report.getSent(), report.getFailedAck(), report.getConsumed(), df.format(successRate),
          report.getPartitions(),
          report.getReplicationFactor(), report.getBrokerCount(), report.getKafkaBrokerRestartCount(),
          report.getMessageSize(), report.getWriteDuration(), report.getReadDuration(), report.getRunDuration()
      };
      formater.addRow(cells);
    }
    System.out.println(formater.getFormatText());
    System.out.println("Total Run Duration: " + totalRunDuration);
  }
}
