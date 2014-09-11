package com.neverwinterdp.scribengin;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.neverwinterdp.scribengin.kafka.ScribenginClusterBuilder;
import com.neverwinterdp.server.shell.Shell;

/**
 * @author Richard Duarte
 */
public class ScribenginClusterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  static protected ScribenginClusterBuilder clusterBuilder;
  static protected Shell shell  ;

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
  
  
  
  @Test
  public void testScribenginCluster() throws Exception {
    createKafkaData();
    Thread.sleep(1000);
  }
}