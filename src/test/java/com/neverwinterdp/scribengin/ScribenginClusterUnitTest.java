package com.neverwinterdp.scribengin;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.SampleEvent;
import com.neverwinterdp.queuengin.kafka.KafkaMessageProducer;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.shell.Shell;
import com.neverwinterdp.util.monitor.ApplicationMonitor;
import com.neverwinterdp.util.monitor.ComponentMonitor;
/**
 * @author Tuan Nguyen
 * @email  tuan08@gmail.com
 */
public class ScribenginClusterUnitTest {
  static {
    System.setProperty("app.dir", "build/cluster") ;
    System.setProperty("app.config.dir", "src/app/config") ;
    System.setProperty("log4j.configuration", "file:src/app/config/log4j.properties") ;
  }
  
  static String TOPIC_NAME = "metrics.consumer" ;
  
  static protected Server      zkServer, kafkaServer, scribenginServer ;
  static protected Shell shell  ;

  @BeforeClass
  static public void setup() throws Exception {
    zkServer = Server.create("-Pserver.name=zookeeper", "-Pserver.roles=zookeeper") ;
    kafkaServer = Server.create("-Pserver.name=kafka", "-Pserver.roles=kafka") ;
    scribenginServer = Server.create("-Pserver.name=scribengin", "-Pserver.roles=scribngin") ;
    
    shell = new Shell() ;
    shell.getShellContext().connect();
  }

  @AfterClass
  static public void teardown() throws Exception {
    scribenginServer.destroy();
    kafkaServer.destroy();
    zkServer.destroy();
  }
  
  @Test
  public void testSendMessage() throws Exception {
    install() ;
    int numOfMessages = 50 ;
    ApplicationMonitor appMonitor = new ApplicationMonitor() ;
    ComponentMonitor monitor = appMonitor.createComponentMonitor(KafkaMessageProducer.class) ;
    KafkaMessageProducer producer = new KafkaMessageProducer(monitor,"127.0.0.1:9092") ;
    for(int i = 0 ; i < numOfMessages; i++) {
      SampleEvent event = new SampleEvent("event-" + i, "event " + i) ;
      Message jsonMessage = new Message("m" + i, event, false) ;
      producer.send(TOPIC_NAME,  jsonMessage) ;
    }
    Thread.sleep(2000) ;
    producer.close();
    uninstall() ;
  }
  
  private void install() throws InterruptedException {
    String installScript =
        "module install " + 
        " -Pmodule.data.drop=true" +
        " -Pzk:clientPort=2181 " +
        " --member-role zookeeper --autostart --module Zookeeper \n" +
        
        "module install " +
        " -Pmodule.data.drop=true" +
        " -Pkafka:port=9092 -Pkafka:zookeeper.connect=127.0.0.1:2181 " +
        " --member-role kafka --autostart --module Kafka \n" +
        
        "module install " +
        " -Pmodule.data.drop=true" +
        " -Pzookeeper-urls=127.0.0.1:2181" + 
        " -Pconsume-topics=" + TOPIC_NAME +
        " --member-role scribengin --autostart --module Scribengin \n" ;
    shell.executeScript(installScript);
    Thread.sleep(1000);
  }
  
  void uninstall() {
    String uninstallScript = 
        "module uninstall --member-role scribengin --timeout 20000 --module Scribengin \n" +
        "module uninstall --member-role kafka --timeout 20000 --module Kafka \n" +
        "module uninstall --member-role zookeeper --timeout 20000 --module Zookeeper";
    shell.executeScript(uninstallScript);
  }
}