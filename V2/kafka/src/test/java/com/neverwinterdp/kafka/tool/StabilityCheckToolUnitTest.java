package com.neverwinterdp.kafka.tool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.server.kafka.KafkaCluster;

public class StabilityCheckToolUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/kafka", 1, 3);
    cluster.setReplication(2);
    cluster.setNumOfPartition(1);
    cluster.start();
    Thread.sleep(2000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testStabilityTool() throws Exception {
    String[] args = {
      "--write-period", "100",
      "--message-size", "500"
    };
    StabilityCheckTool.main(args);
  }
}
