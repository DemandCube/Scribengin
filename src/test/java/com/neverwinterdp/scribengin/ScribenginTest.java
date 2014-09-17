package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.lang.reflect.Method;

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
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.neverwinterdp.scribengin.kafka.ScribenginClusterBuilder;
import com.neverwinterdp.scribengin.kafka.SupportClusterBuilder;
import com.neverwinterdp.scribengin.scribengin.Scribengin;

/**
 * @author Richard Duarte
 */
public class ScribenginTest {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  static int numOfMessages = 100 ;
  static protected SupportClusterBuilder supportClusterBuilder;
  private static List<String> topics;
  
  @BeforeClass
  static public void setup() throws Exception {
    supportClusterBuilder = new SupportClusterBuilder();
    supportClusterBuilder.install();
  }

  @AfterClass
  static public void teardown() throws Exception {
    supportClusterBuilder.uninstall();
    supportClusterBuilder.destroy();
  }
  
  private static void createKafkaData(String topic){
    //Write numOfMessages to Kafka
    Properties producerProps = new Properties();
    producerProps.put("metadata.broker.list", "localhost:9092");
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    producerProps.put("request.required.acks", "1");
    
    Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerProps));
    for(int i =0 ; i < numOfMessages; i++) {
      //TODO FIX TOPICS
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic,topic+"Neverwinter"+Integer.toString(i));
      producer.send(data);
    }
    producer.close();
  }
  
  /**
   * Read in file, return whole file as a string
   * @param hdfsPath Path of HDFS file to read
   * @return whole file as a string
   */
  private void assertHDFSmatchesKafka(String hdfsPath, String topic) {
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
      assertionString += topic+"Neverwinter"+Integer.toString(i);
    }
    assertEquals("Data passed into Kafka did not match what was read from HDFS",assertionString,readLine);
    
  }

  
  @Test
  public void testScribenginRecovery() throws Exception {
    topics = new LinkedList<String>();
    topics.add("cluster.test0");
    topics.add("cluster.test1");
    topics.add("cluster.test2");
    for(String t : topics){
      createKafkaData(t);
    }
    
    Scribengin s = new Scribengin(topics, "127.0.0.1",9092,5000);
    s.start();
    Method f = s.getClass().getDeclaredMethod("killWorker", new Class[]{int.class});
    f.setAccessible(true);
    //Kill the first worker
    f.invoke(s,0);
    
    Thread.sleep(10000);
    for(String t : topics){
      assertHDFSmatchesKafka(supportClusterBuilder.getHadoopConnection(), t);
    }
  }
}