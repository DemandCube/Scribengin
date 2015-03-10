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
    AckKafkaWriterTestRunner runner = new AckKafkaWriterTestRunner() ;
    runner.setUp();
    runner.run();
    runner.tearDown();
    runner.getReport().print(System.out, "Test AckKafkaWriter");
  }
}