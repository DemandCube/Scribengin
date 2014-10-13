package com.neverwinterdp.scribengin.cluster;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.neverwinterdp.scribengin.ScribeMaster;
import com.neverwinterdp.scribengin.ScribeConsumerManager.ClusterScribeConsumerManager;
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;

/**
 * Brings up scribengin cluster
 * @author Richard Duarte
 *
 */
public class ScribeMasterRestartDistributedTest{
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  private static ScribeConsumerClusterTestHelper helper = new ScribeConsumerClusterTestHelper();
  static int numOfMessages = 100 ;
  private static final Logger LOG = Logger.getLogger(ScribeMasterRestartDistributedTest.class.getName());
  private static ScribeMaster sm;
  
  @BeforeClass
  static public void setup() throws Exception {
    helper.setup();
  }

  @AfterClass
  static public void teardown() throws Exception {
    try{
      sm.stop();
    } catch(Exception e){}
    helper.teardown();
  }
  
  
  @Test
  public void ScribeMasterRestartDevTest() throws InterruptedException{
    
    ScribeConsumerConfig c = new ScribeConsumerConfig();
    c.hdfsPath = helper.getHadoopConnection();
    c.topic = helper.getTopic();
    c.cleanStart = true;
    List<HostPort> bList = new LinkedList<HostPort>();
    bList.add(new HostPort("127.0.0.1","9092"));
    c.brokerList = bList;
    
    LinkedList<String> topics = new LinkedList<String>();
    topics.push(helper.getTopic());
    
    sm = new ScribeMaster(topics, c);
    sm.setScribeConsumerManager(new ClusterScribeConsumerManager());
    sm.start();
    
    sm.checkOnConsumersThreaded(1000);
    
    
    Thread.sleep(2000);
    
    LOG.info("Creating kafka data");
    //Create kafka data
    helper.createKafkaData(0);
      
    //Wait for consumption
    Thread.sleep(5000);
    //Ensure messages 0-99 were consumed
    LOG.info("Asserting data is correct");
    helper.assertHDFSmatchesKafka(0,helper.getHadoopConnection());
    
    LOG.info("Killing consumers!");
    sm.killConsumersForceRestart();
    
    
    LOG.info("Creating kafka data");
    //Create kafka data
    helper.createKafkaData(100);
      
    //Wait for consumption
    Thread.sleep(5000);
    //Ensure messages 100-199 were consumed
    LOG.info("Asserting data is correct");
    helper.assertHDFSmatchesKafka(100,helper.getHadoopConnection());
    
    
  }
}