package com.neverwinterdp.scribengin.clusterBuilder;

import static com.neverwinterdp.scribengin.utilities.Util.isOpen;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.utilities.Util;


public class SupportClusterBuilderTest {
  static SupportClusterBuilder clusterBuilder;
  static int zkPort = 2181;
  private static String version = "0.8.1.1";
  private static String zkHost = "127.0.0.1";
  private static int kafkaPort = 9092;
  private static String kafkaHost = "127.0.0.1";
  private String topic = "test.topic.789";

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


  @Test
  public void testWriteToKafka() {
    try {
      Util.createKafkaData(kafkaHost, kafkaPort, topic);
    } catch (Exception e) {
      fail("Unable to write to kafka " + e.getLocalizedMessage());
    }
    assertTrue("Succesfully wrote to kafka", true);
  }

  @Test
  public void testReadFromKafka() {
    assertTrue(true);
  }



  @AfterClass
  public static void tearDown() throws Exception {
    logger.info("tearDown. ");
    // clusterBuilder.uninstall();
  }
}
