package com.neverwinterdp.kafka.producer;

import org.junit.Test;

public class AckKafkaWriterPerfomanceTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }
  
  @Test
  public void testRunner() throws Exception {
    String[][] args = {
      {"--topic", "hello", "--message-size", "1024", "--max-num-message", "30000"},
      {"--topic", "hello", "--message-size", "4092", "--max-num-message", "30000"}
    };
    
    AckKafkaWriterTestRunner.Report[] reports = new AckKafkaWriterTestRunner.Report[args.length];
    for(int i = 0; i < args.length; i++) {
      AckKafkaWriterTestRunnerConfig config = new AckKafkaWriterTestRunnerConfig(args[i]);
      AckKafkaWriterTestRunner runner = new AckKafkaWriterTestRunner(config) ;
      runner.setUp();
      runner.run();
      reports[i] = runner.getReport();
      runner.tearDown();
      runner.getReport().print(System.out, "Test AckKafkaWriter");
    }
    //TODO: agregate the report
  }
}
