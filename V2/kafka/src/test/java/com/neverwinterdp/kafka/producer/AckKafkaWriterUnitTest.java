package com.neverwinterdp.kafka.producer;

import org.junit.Test;
/**
 * @author Tuan
 */
public class AckKafkaWriterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }

  @Test
  public void testRunner() throws Exception {
    String[] args = {
     "--topic", "hello", "--message-size", "1024", "--max-num-message", "30000"
    };
    AckKafkaWriterTestRunnerConfig config = new AckKafkaWriterTestRunnerConfig(args);
    AckKafkaWriterTestRunner runner = new AckKafkaWriterTestRunner(config) ;
    runner.setUp();
    runner.run();
    runner.tearDown();
    runner.getReport().print(System.out, "Test AckKafkaWriter");
  }
}