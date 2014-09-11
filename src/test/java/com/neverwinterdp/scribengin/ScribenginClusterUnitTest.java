package com.neverwinterdp.scribengin;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


import com.neverwinterdp.scribengin.kafka.KafkaClusterBuilder;
import com.neverwinterdp.scribengin.kafka.SimplePartitioner;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;

/**
 * @author Richard Duarte
 */
public class ScribenginClusterUnitTest {
  static {
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  static protected KafkaClusterBuilder clusterBuilder;
  static protected Server scribenginServer ;
  static protected Shell shell  ;

  @BeforeClass
  static public void setup() throws Exception {
    clusterBuilder = new KafkaClusterBuilder() ;
    clusterBuilder.install();
    createKafkaData();
  }

  @AfterClass
  static public void teardown() throws Exception {
    uninstallScribengin();
    scribenginServer.destroy();
    clusterBuilder.destroy();
  }
  
  private static void createKafkaData(){
    //Write numOfMessages to Kafka
    int numOfMessages = 100 ;
    
    Properties producerProps = new Properties();
    producerProps.put("metadata.broker.list", "localhost:9092");
    producerProps.put("serializer.class", "kafka.serializer.StringEncoder");
    producerProps.put("partitioner.class", SimplePartitioner.class.getName());
    producerProps.put("request.required.acks", "1");
    
    Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(producerProps));
    for(int i =0 ; i < numOfMessages; i++) {
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(KafkaClusterBuilder.TOPIC,"Neverwinter"+Integer.toString(i));
      producer.send(data);
    }
    producer.close();
    
    
  }
  
  private static void installScribengin() throws InterruptedException{
    String scribeInstallScript = 
        "module install " + 
        " -Pmodule.data.drop=true" +
        " -Pscribengin:checkpointinterval=200" +
        " -Pscribengin:leader=127.0.0.1:2181" +
        " -Pscribengin:partition=0" +
        " -Pscribengin:topic="+KafkaClusterBuilder.TOPIC +
        " --member-role scribengin --autostart --module Scribengin \n"; 
        
    shell.executeScript(scribeInstallScript);
    Thread.sleep(5000);
    
  }
  
  static void uninstallScribengin() {
    String uninstallScript = 
        "module uninstall --member-role scribengin --timeout 20000 --module Scribengin \n" ;
    shell.executeScript(uninstallScript);
  }
  
  @Test
  public void testScribenginCluster() throws Exception {
    scribenginServer = Server.create("-Pserver.name=scribengin", "-Pserver.roles=scribengin");
    shell = new Shell() ;
    shell.getShellContext().connect();
    installScribengin();
  }
  
}