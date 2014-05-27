package com.neverwinterdp.scribengin;

import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.neverwinterdp.message.Message;
import com.neverwinterdp.message.SampleEvent;
import com.neverwinterdp.queuengin.kafka.KafkaMessageProducer;
import com.neverwinterdp.queuengin.kafka.cluster.KafkaServiceModule;
import com.neverwinterdp.queuengin.kafka.cluster.ZookeeperServiceModule;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.cluster.ClusterClient;
import com.neverwinterdp.server.cluster.ClusterMember;
import com.neverwinterdp.server.cluster.hazelcast.HazelcastClusterClient;
import com.neverwinterdp.util.FileUtil;
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
  
  static String TOPIC_NAME = "scribengin" ;
  
  static protected Server      zkServer, kafkaServer, scribenginServer ;
  static protected ClusterClient client ;

  @BeforeClass
  static public void setup() throws Exception {
    FileUtil.removeIfExist("build/cluster", false);
    Properties zkServerProps = new Properties() ;
    zkServerProps.put("server.group", "NeverwinterDP") ;
    zkServerProps.put("server.cluster-framework", "hazelcast") ;
    zkServerProps.put("server.roles", "master") ;
    zkServerProps.put("server.service-module", ZookeeperServiceModule.class.getName()) ;
    //zkServerProps.put("zookeeper.config-path", "") ;
    zkServer = Server.create(zkServerProps);
    
    Properties kafkaServerProps = new Properties() ;
    kafkaServerProps.put("server.group", "NeverwinterDP") ;
    kafkaServerProps.put("server.cluster-framework", "hazelcast") ;
    kafkaServerProps.put("server.roles", "master") ;
    kafkaServerProps.put("server.service-module", KafkaServiceModule.class.getName()) ;
    kafkaServerProps.put("kafka.zookeeper-urls", "127.0.0.1:2181") ;
    //kafkaServerProps.put("kafka.consumer-report.topics", TOPIC_NAME) ;
    kafkaServer = Server.create(kafkaServerProps);
    
    Properties scribenginServerProps = new Properties() ;
    scribenginServerProps.put("server.group", "NeverwinterDP") ;
    scribenginServerProps.put("server.cluster-framework", "hazelcast") ;
    scribenginServerProps.put("server.roles", "master") ;
    scribenginServerProps.put("server.service-module", ScribenginServiceModule.class.getName()) ;
    scribenginServerProps.put("kafka.zookeeper-urls", "127.0.0.1:2181") ;
    scribenginServerProps.put("scribengin.consume-topics", TOPIC_NAME) ;
    scribenginServer = Server.create(scribenginServerProps);
    
    ClusterMember member = zkServer.getClusterService().getMember() ;
    String connectUrl = member.getIpAddress() + ":" + member.getPort() ;
    client = new HazelcastClusterClient(connectUrl) ;
  }

  @AfterClass
  static public void teardown() throws Exception {
    client.shutdown(); 
    scribenginServer.exit(0);
    kafkaServer.exit(0);
    zkServer.exit(0) ;
  }
  
  @Test
  public void testSendMessage() throws Exception {
    int numOfMessages = 50 ;
    KafkaMessageProducer producer = new KafkaMessageProducer("127.0.0.1:9092") ;
    for(int i = 0 ; i < numOfMessages; i++) {
      SampleEvent event = new SampleEvent("event-" + i, "event " + i) ;
      Message jsonMessage = new Message("m" + i, event, false) ;
      producer.send(TOPIC_NAME,  jsonMessage) ;
    }
    Thread.sleep(2000) ;
  }
}