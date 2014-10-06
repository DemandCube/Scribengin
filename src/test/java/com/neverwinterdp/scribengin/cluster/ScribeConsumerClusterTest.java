package com.neverwinterdp.scribengin.cluster;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;

/**
 * Brings up scribengin cluster
 * @author Richard Duarte
 *
 */
public class ScribeConsumerClusterTest {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  private static ScribeConsumerClusterTestHelper helper = new ScribeConsumerClusterTestHelper();
  static int numOfMessages = 100 ;
  private static final Logger LOG = Logger.getLogger(ScribeConsumerClusterTest.class.getName());
  private static Server scribeConsumer;
  
  
  @BeforeClass
  static public void setup() throws Exception {
    helper.setup();
  }

  @AfterClass
  static public void teardown() throws Exception {
    helper.teardown();
    try{
      scribeConsumer.destroy();
    } catch(Exception e){}
  }
  
 

  @Test
  public void TestScribeConsumerCluster() throws InterruptedException{
    
    //Bring up scribeConsumer
    scribeConsumer = Server.create("-Pserver.name=scribeconsumer", "-Pserver.roles=scribeconsumer");
    Shell shell = new Shell() ;
    shell.getShellContext().connect();
    shell.execute("module list --type available");
    
    String installScript ="module install " + 
        " -Pmodule.data.drop=true" +
        " -Pscribeconsumer:precommitpathprefix=/tmp" +
        " -Pscribeconsumer:commitpathprefix=/committed" +
        " -Pscribeconsumer:topic="+ helper.getTopic() +
        " -Pscribeconsumer:partition=0" +
        " -Pscribeconsumer:brokerList=127.0.0.1:9092" +
        " -Pscribeconsumer:commitCheckPointInterval=500"+
        " -Pscribeconsumer:hdfsPath="+helper.getHadoopConnection()+
        " -Pscribeconsumer:cleanStart=True"+
        " --member-role scribeconsumer --autostart --module ScribeConsumer \n";
    shell.executeScript(installScript);
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