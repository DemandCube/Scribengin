package com.neverwinterdp.kafka.producer;

import org.junit.Test;
//TODO success rate 2 dp
//TODO add total time for each run
//TODO add 
import com.google.common.base.Stopwatch;

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
    for (int i = 0; i < args.length; i++) {
      AckKafkaWriterTestRunner runner = new AckKafkaWriterTestRunner(args[i]);
      runner.setUp();
      runner.run();
      runner.tearDown();
    }
    totalRunDuration.stop();
  }
}
