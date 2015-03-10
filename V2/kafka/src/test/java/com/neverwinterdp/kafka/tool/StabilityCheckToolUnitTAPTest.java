package com.neverwinterdp.kafka.tool;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.server.kafka.KafkaCluster;
import com.neverwinterdp.util.FileUtil;

public class StabilityCheckToolUnitTAPTest {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    FileUtil.removeIfExist("./build/cluster", false);
    cluster = new KafkaCluster("./build/cluster", 1, 3);
    cluster.start();
    Thread.sleep(5000);
  }
  
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testStabilityTool() throws Exception {
    String[] args = {
      "--zk-connect", cluster.getZKConnect(),
      "--write-period", "10",
      "--message-size", "500",
      "--num-partition", "3",
      "--exit-wait-time", "5000",
      "--max-message-per-partition", "100",
      "--max-duration", "60000",
      "--replication", "3",
      "--tap-enable",
    };
    StabilityCheckTool.main(args);
    Thread.sleep(3000);
  }
}
