package com.neverwinterdp.scribengin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
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
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.neverwinterdp.scribengin.kafka.SupportClusterBuilder;
import com.neverwinterdp.scribengin.ScribeConsumer;

/**
 * @author Richard Duarte
 */
public class ScribeConsumerRestartTest {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  static String TOPIC = "cluster.test";
  static int numOfMessages = 100 ;
  static protected SupportClusterBuilder supportClusterBuilder;

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
        System.err.println("PATH: ");
        System.err.println(p);
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
    
    List<HostPort> brokerList = new LinkedList<HostPort>();
    brokerList.add(new HostPort("localhost",9092));
    
    ScribeConsumer sw = new ScribeConsumer("/tmp",  //pre commit path
                                           "/committed",  //commit path
                                           TOPIC,   //kafka topic
                                           0,       //kafka partition
                                           brokerList,  //list of kafkas
                                           200,  //check interval
                                           supportClusterBuilder.getHadoopConnection());  //connection to hadoop
    sw.init();
    sw.cleanStart(true);
    sw.start();

    //Create kafka data
    createKafkaData(0);
      
    //Wait for consumption
    Thread.sleep(5000);
    //Ensure messages 0-100 were consumed
    assertHDFSmatchesKafka(0,supportClusterBuilder.getHadoopConnection());
    
    //Kill the worker
    sw.stop();
    
    Thread.sleep(10000);
    
    //Create new worker
    ScribeConsumer sw2 = new ScribeConsumer("/tmp",  //pre commit path
                                            "/committed",  //commit path
                                            TOPIC,   //kafka topic
                                            0,       //kafka partition
                                            brokerList,  //list of kafkas
                                            200,  //check interval
                                            supportClusterBuilder.getHadoopConnection());  //connection to hadoop
    sw2.init();
    sw2.start();
    Thread.sleep(2000);
    
    //Create data starting at message 100
    createKafkaData(100);
    
    //Wait for data to be consumed
    Thread.sleep(5000);
    
    sw2.stop();
    
    //Wait for thread to die to avoid closed filesystem errors
    Thread.sleep(5000);
    
    //Ensure all the data is there and in the correct order
    assertHDFSmatchesKafka(100,supportClusterBuilder.getHadoopConnection());
  }
}