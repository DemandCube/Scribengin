package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.neverwinterdp.scribengin.kafka.ScribenginClusterBuilder;

/**
 * @author Richard Duarte
 */
public class ScribenginClusterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
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
    int numOfMessages = 100 ;
    
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
  private String getFileHDFS(String hdfsPath) {
    String readLine="";
    String tempLine="";
    try {
      FileSystem fs = FileSystem.get(URI.create(hdfsPath), new Configuration());
      Path path = new Path(hdfsPath);
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
      while((tempLine = br.readLine() ) != null){
        readLine+=tempLine;
      }
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue("Could not read from HDFS", false);
    }
    return readLine;
  }

  
  @Test
  public void testScribenginCluster() throws Exception {
    createKafkaData();
    Thread.sleep(5000);
    //getFileHDFS();
  }
}