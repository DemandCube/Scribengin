package com.neverwinterdp.scribengin.cluster;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribeConsumerManager.ClusterScribeConsumerManager;
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;

/**
 * Brings up scribengin cluster
 * @author Richard Duarte
 *
 */
public class ClusterScribeConsumerManagerTest{
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  private static ScribeConsumerClusterTestHelper helper = new ScribeConsumerClusterTestHelper();
  static int numOfMessages = 100 ;
  private static final Logger LOG = Logger.getLogger(ClusterScribeConsumerManagerTest.class.getName());
  private static ClusterScribeConsumerManager manager;
  
  
  @BeforeClass
  static public void setup() throws Exception {
    helper.setup();
  }

  @AfterClass
  static public void teardown() throws Exception {
    helper.teardown();
    try{
      manager.shutdownConsumers();
    } catch(Exception e){}
  }
  
  

  @Test
  public void ScribeConsumerClusterTest() throws InterruptedException{
    manager = new ClusterScribeConsumerManager();
    ScribeConsumerConfig c = new ScribeConsumerConfig();
    c.hdfsPath = helper.getHadoopConnection();
    c.topic = helper.getTopic();
    List<HostPort> bList = new LinkedList<HostPort>();
    bList.add(new HostPort("127.0.0.1","9092"));
    c.brokerList = bList;
    
    manager.startNewConsumer(c);
    
    Thread.sleep(2000);
    
    LOG.info("Creating kafka data");
    //Create kafka data
    helper.createKafkaData(0);
      
    //Wait for consumption
    Thread.sleep(5000);
    //Ensure messages 0-100 were consumed
    LOG.info("Asserting data is correct");
    helper.assertHDFSmatchesKafka(0,helper.getHadoopConnection());
  }
}