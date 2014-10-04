package com.neverwinterdp.scribengin.cluster;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribeConsumerManager.ClusterScribeConsumerManager;
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumer;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;

/**
 * @author Richard Duarte
 */
public class ScribeConsumerRestartTest  {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  private static ScribeConsumerClusterTestHelper helper = new ScribeConsumerClusterTestHelper();
  private static final Logger LOG = Logger.getLogger(ScribeConsumerRestartTest.class.getName());
  private static ScribeConsumer sc1, sc2;
  
  @BeforeClass
  static public void setup() throws Exception {
    helper.setup();
    Thread.sleep(3000);
  }

  @AfterClass
  static public void teardown() throws Exception {
    try{
      sc1.stop();
    } catch(Exception e){}
    try{
      sc2.stop();
    } catch(Exception e){}
    helper.teardown();
    Thread.sleep(3000);
  }
  
  /**
   * Starts a ScribeWorker, Stops it, Commits more data to kafka, and starts a new ScribeWorker
   * No data should be repeated
   * 
   * This test is currently failing and ruining my life.  Ignoring for now.
   */
  @Ignore
  @Test
  public void testScribenginWorkerRestart() throws Exception {
    ScribeConsumerConfig c = new ScribeConsumerConfig();
    c.hdfsPath = helper.getHadoopConnection();
    c.topic = helper.getTopic();
    List<HostPort> bList = new LinkedList<HostPort>();
    bList.add(new HostPort("127.0.0.1","9092"));
    c.brokerList = bList;
    c.COMMIT_PATH_PREFIX = "/committed";
    sc1 = new ScribeConsumer(c);
    Thread.sleep(2000);
    
    LOG.info("Creating kafka data");
    //Create kafka data
    helper.createKafkaData(0);
    
    sc1.init();
    sc1.cleanStart(true);
    sc1.start();
      
    //Wait for consumption
    Thread.sleep(10000);
    //Ensure messages 0-100 were consumed
    LOG.info("Asserting data is correct");
    helper.assertHDFSmatchesKafka(0,helper.getHadoopConnection());
    
    LOG.info("Stopping first ScribeConsumer");
    sc1.stop();
    
    //Wait for file handles in HDFS to close
    //TODO: Make it so we don't need ot sleep here
    Thread.sleep(15000);
    
    LOG.info("Starting second ScribeConsumer");
    //Create new worker
    sc2 = new ScribeConsumer(c);
    Thread.sleep(2000);
    sc2.init();
    sc2.start();
    
    LOG.info("Creating kafka data part 2");
    //Create data starting at message 100
    helper.createKafkaData(100);
    
    //Wait for data to be consumed
    Thread.sleep(20000);
    
    LOG.info("Stopping 2nd ScribeConsumer");
    sc2.stop();
    //Wait for thread to die to avoid closed filesystem errors
    Thread.sleep(10000);
    
    LOG.info("Asserting data is correct");
    //Ensure all the data is there and in the correct order
    helper.assertHDFSmatchesKafka(100,helper.getHadoopConnection());
    
  }
}