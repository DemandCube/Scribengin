package com.neverwinterdp.scribengin;

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
import com.neverwinterdp.scribengin.kafka.ScribenginClusterBuilder;

/**
 * @author Richard Duarte
 */
public class ScribenginClusterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  static int numOfMessages = 100 ;
  static protected ScribenginClusterBuilder clusterBuilder;

  @BeforeClass
  static public void setup() throws Exception {
    clusterBuilder = new ScribenginClusterBuilder() ;
    clusterBuilder.install();
  }

  @AfterClass
  static public void teardown() throws Exception {
    clusterBuilder.uninstall();
    clusterBuilder.destroy();
  }
  
  private static void createKafkaData(){
    //Write numOfMessages to Kafka
    Properties producerProps = new Properties();
    producerProps.put("metadata.broker.list", "localhost:9092");
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    producerProps.put("request.required.acks", "1");
    
    Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerProps));
    for(int i =0 ; i < numOfMessages; i++) {
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(ScribenginClusterBuilder.TOPIC,"Neverwinter"+Integer.toString(i));
      producer.send(data);
    }
    producer.close();
  }
  
  /**
   * Read in file, return whole file as a string
   * @param hdfsPath Path of HDFS file to read
   * @return whole file as a string
   */
  private void assertHDFSmatchesKafka(String hdfsPath) {
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
    for(int i=0; i< numOfMessages; i++){
      assertionString += "Neverwinter"+Integer.toString(i);
    }
    assertEquals("Data passed into Kafka did not match what was read from HDFS",assertionString,readLine);
    
  }

  
  @Test
  public void testScribenginCluster() throws Exception {
    createKafkaData();
    Thread.sleep(15000);
    assertHDFSmatchesKafka(clusterBuilder.getHadoopConnection());
  }
}