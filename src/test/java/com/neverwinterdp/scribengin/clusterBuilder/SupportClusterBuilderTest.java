package com.neverwinterdp.scribengin.clusterBuilder;

import static com.neverwinterdp.scribengin.utilities.Util.isOpen;
import static org.junit.Assert.assertTrue;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class SupportClusterBuilderTest {
  static SupportClusterBuilder clusterBuilder;
  static int zkPort = 2181;
  private static String version = "0.8.1.1";
  private static String zkHost = "127.0.0.1";
  private static int kafkaPort = 9091;
  private static String kafkaHost = "127.0.0.1";

  private static final Logger logger = Logger.getLogger(SupportClusterBuilderTest.class);

  @BeforeClass
  public static void setup() {
    try {
      clusterBuilder = new SupportClusterBuilder(version, zkHost, zkPort, kafkaHost, kafkaPort);
      clusterBuilder.install();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Test
  public void testZkPortIsOpen() {
    assertTrue(isOpen(zkPort));
  }

  @Test
  public void testKafkaPortIsOpen() {
    assertTrue(isOpen(kafkaPort));
  }


  @AfterClass
  public static void tearDown() throws Exception {
    logger.info("tearDown. ");
    clusterBuilder.uninstall();
  }
}
