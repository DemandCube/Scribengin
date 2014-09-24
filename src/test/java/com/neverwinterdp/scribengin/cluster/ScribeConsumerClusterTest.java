package com.neverwinterdp.scribengin.cluster;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.scribengin.clusterBuilder.SupportClusterBuilder;
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
  
  static String TOPIC = "cluster.test";
  static int numOfMessages = 100 ;
  static protected SupportClusterBuilder supportClusterBuilder;
  private static final Logger LOG = Logger.getLogger(ScribeConsumerClusterTest.class.getName());
  private static Server scribeConsumer;
  
  
  @BeforeClass
  static public void setup() throws Exception {
    supportClusterBuilder = new SupportClusterBuilder();
    supportClusterBuilder.install();
  }

  @AfterClass
  static public void teardown() throws Exception {
    try{
      supportClusterBuilder.uninstall();
      supportClusterBuilder.destroy();
    } catch(Exception e){}
    try{
      scribeConsumer.destroy();
    } catch(Exception e){}
  }
  
  private static void createKafkaData(int startNum){
    //Write numOfMessages to Kafka
    Properties producerProps = new Properties();
    producerProps.put("metadata.broker.list", "localhost:9092");
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    producerProps.put("request.required.acks", "1");
    
    Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerProps));
    for(int i =startNum ; i < startNum+numOfMessages; i++) {
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC,"Neverwinter"+Integer.toString(i));
      producer.send(data);
    }
    producer.close();
  }
  
  /**
   * Read in file, return whole file as a string
   * @param hdfsPath Path of HDFS file to read
   * @return whole file as a string
   */
  private void assertHDFSmatchesKafka(int startNum, String hdfsPath) {
    //int count = 0;
    String tempLine="";
    String readLine="";
    try {
      FileSystem fs = FileSystem.get(URI.create(hdfsPath), new Configuration());
      Path directory = new Path("/committed");
      RemoteIterator<LocatedFileStatus> directoryIterator = fs.listFiles(directory,false);
      while(directoryIterator.hasNext()){
        Path p = directoryIterator.next().getPath();
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
         while((tempLine = br.readLine() ) != null){
           readLine += tempLine;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Could not read from HDFS", false);
    }
    
    //Build string that will match
    String assertionString = "";
    for(int i=0; i< startNum+numOfMessages; i++){
      assertionString += "Neverwinter"+Integer.toString(i);
    }
    assertEquals("Data passed into Kafka did not match what was read from HDFS",assertionString,readLine);
  }

  @Test
  public void ScribeWorkerClusterTest() throws InterruptedException{
    
    //Bring up scribeConsumer
    scribeConsumer = Server.create("-Pserver.name=scribeconsumer", "-Pserver.roles=scribeconsumer");
    Shell shell = new Shell() ;
    shell.getShellContext().connect();
    shell.execute("module list --type available");
    
    String installScript ="module install " + 
        " -Pmodule.data.drop=true" +
        " -Pscribeconsumer:precommitpathprefix=/tmp" +
        " -Pscribeconsumer:commitpathprefix=/committed" +
        " -Pscribeconsumer:topic="+ TOPIC +
        " -Pscribeconsumer:partition=0" +
        " -Pscribeconsumer:brokerList=127.0.0.1:9092" +
        " -Pscribeconsumer:commitCheckPointInterval=500"+
        " -Pscribeconsumer:hdfsPath="+supportClusterBuilder.getHadoopConnection()+
        " -Pscribeconsumer:cleanStart=True"+
        " --member-role scribeconsumer --autostart --module ScribeConsumer \n";
    shell.executeScript(installScript);
    Thread.sleep(2000);
    
    LOG.info("Creating kafka data");
    //Create kafka data
    createKafkaData(0);
      
    //Wait for consumption
    Thread.sleep(5000);
    //Ensure messages 0-100 were consumed
    LOG.info("Asserting data is correct");
    assertHDFSmatchesKafka(0,supportClusterBuilder.getHadoopConnection());
  }
}