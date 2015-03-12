package com.neverwinterdp.kafka.producer;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

//TODO success rate 2 dp
//TODO add total time for each run
//TODO add 


import com.google.common.base.Stopwatch;
import com.neverwinterdp.kafka.producer.AckKafkaWriterTestRunner.Report;
import com.neverwinterdp.util.text.TabularFormater;

public class AckKafkaWriterPerfomanceTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  private Stopwatch totalRunDuration = Stopwatch.createUnstarted();
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
        { "--topic", "hello", "--message-size", "512000", "--max-num-message", "10000", "--num-partition", "2","--num-replication", "3","--num-kafka-brokers", "3" }       
    };
    totalRunDuration.start();
    AckKafkaWriterTestRunner.Report[] reports = new AckKafkaWriterTestRunner.Report[args.length];
    for (int i = 0; i < args.length; i++) {
      AckKafkaWriterTestRunnerConfig config = new AckKafkaWriterTestRunnerConfig(args[i]);
      AckKafkaWriterTestRunner runner = new AckKafkaWriterTestRunner(config);
      runner.setUp();
      runner.run();
      reports[i] = runner.getReport();
      runner.tearDown();
    }
    totalRunDuration.stop();
    formatReports("Perfomance Tests", reports);
  }

  private void formatReports(String title, Report[] reports) {
    String[] header = { "Sent", "failed", "consumed", "success%", "part", "repl", "brokers", "restarts","bytes", "writeDur","w/s", "readDur","r/s", "runDur" };
    TabularFormater formater = new TabularFormater(header);
    formater.setTitle(title);
    DecimalFormat df = new DecimalFormat("0.00");
    DecimalFormat df2 = new DecimalFormat("0");
    double successRate = 0.0d;
    double writePerSec=0d;
    double readPerSec=0d;
    
    for (Report report : reports) {
      successRate = report.getConsumed() * 100d / report.getSent();
      writePerSec = report.getSent() /(report.getWriteDuration().elapsed(TimeUnit.MILLISECONDS)/1000d);
      readPerSec = report.getConsumed() /(report.getReadDuration().elapsed(TimeUnit.MILLISECONDS)/1000d);
      Object[] cells = {
          report.getSent(), report.getFailedAck(), report.getConsumed(), df.format(successRate), report.getPartitions(),
          report.getReplicationFactor(), report.getBrokerCount(), report.getKafkaBrokerRestartCount(),
          report.getMessageSize(), report.getWriteDuration(), df2.format(writePerSec), report.getReadDuration(),df2.format(readPerSec) , report.getRunDuration()
      };
      formater.addRow(cells);
    }
    System.out.println(formater.getFormatText());
   System.out.println("Total Run Duration: " + totalRunDuration);
   System.out.println();
   System.out.println("part - number of partitions per topic");   
   System.out.println("repl - topic replication factor");
   System.out.println("w/s - writes/second");
   System.out.println("r/s - reads/second");
   System.out.println("writeDur - writer run duration");
   System.out.println("readDur - consumer run duration");
   System.out.println("runDur - run duration for the entire test");
  }
}
