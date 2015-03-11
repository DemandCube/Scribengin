package com.neverwinterdp.kafka.producer;

import org.junit.Test;
//TODO move reporting method to report
//TODO short names for formatter
//TODO remove stopwatch import

import com.neverwinterdp.kafka.producer.AckKafkaWriterTestRunner.Report;
import com.neverwinterdp.util.text.TabularFormater;

public class AckKafkaWriterPerfomanceTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  @Test
  public void testRunner() throws Exception {
    String[][] args = {
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "30000" },
        { "--topic", "hello", "--message-size", "4096", "--max-num-message", "30000" },
        { "--topic", "hello", "--message-size", "1024", "--max-num-message", "30000", "--num-partition", "10" },
        { "--topic", "hello", "--message-size", "2048", "--max-num-message", "50000", "--num-partition", "2","--num-replication", "1" }
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

  void formatReports(String title, Report[] reports) {
    TabularFormater formater = new TabularFormater("Sent", "failed", "consumed", "partitions","replication", "restarts", "bytes",
        "writeDuration", "readDuration");
    formater.setTitle(title);
    for (Report report : reports) {
      formater.addRow(report.getSent(), report.getFailedAck(), report.getConsumed(), report.getPartitions(),report.getReplicationFactor(),
          report.getKafkaBrokerRestartCount(), report.getMessageSize(), report.getWriteDuration(),
          report.getReadDuration());
    }
    System.out.println(formater.getFormatText());
  }
}
