package com.neverwinterdp.scribengin.scribeworker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.neverwinterdp.queuengin.kafka.SimplePartitioner;
import com.neverwinterdp.scribengin.kafka.ScribenginWorkerClusterBuilder;
import com.neverwinterdp.scribengin.kafka.SupportClusterBuilder;
import com.neverwinterdp.scribengin.scribeworker.ScribeWorker;
import com.neverwinterdp.scribengin.scribeworker.config.ScribeWorkerConfig;

/**
 * @author Richard Duarte
 */
public class ScribeWorkerRestartUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  static int numOfMessages = 100 ;
  //static protected ScribenginWorkerClusterBuilder scribenginWorkerClusterBuilder;
  static protected SupportClusterBuilder supportClusterBuilder;

  @BeforeClass
  static public void setup() throws Exception {
    supportClusterBuilder = new SupportClusterBuilder();
    supportClusterBuilder.install();
    
    
  }

  @AfterClass
  static public void teardown() throws Exception {
    //scribenginWorkerClusterBuilder.uninstall();
    //scribenginWorkerClusterBuilder.destroy();
    try{
      supportClusterBuilder.uninstall();
      supportClusterBuilder.destroy();
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
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(ScribenginWorkerClusterBuilder.TOPIC,"Neverwinter"+Integer.toString(i));
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

  /**
   * Starts a ScribeWorker, Stops it, Commits more data to kafka, and starts a new ScribeWorker
   * No data should be repeated
   */
  @Test
  public void testScribenginWorkerRestart() throws Exception {
    //Start ScribeWorker
    ScribeWorkerConfig c = new ScribeWorkerConfig("localhost",    //kafka address
                                                  9092,           //kafka port
                                                  "cluster.test", //topic
                                                  "/tmp",         //temporary data folder
                                                  "/committed",   //committed data path
                                                  0,              //kafka partition
                                                  supportClusterBuilder.getHadoopConnection(), //hadoop connection
                                                  100);           //Checking interval
    ScribeWorker sw = new ScribeWorker(c);
    sw.start();
    
    //Create kafka data
    createKafkaData(0);
    //Wait for consumption
    Thread.sleep(15000);
    //Ensure messages 0-100 were consumed
    assertHDFSmatchesKafka(0,supportClusterBuilder.getHadoopConnection());
    
    //Kill the worker
    sw.stop();
    
    //Create data starting at message 100
    createKafkaData(100);
    
    //Create new worker
    ScribeWorker sw2 = new ScribeWorker(c);
    sw2.start();
    
    //Wait for data to be consumed
    Thread.sleep(35000);
    
    sw2.stop();
    
    //Ensure all the data is there and in the correct order
    assertHDFSmatchesKafka(100,supportClusterBuilder.getHadoopConnection());
  }
}