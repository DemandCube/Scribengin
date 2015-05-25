package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.kafka.tool.KafkaClusterTool;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaMessageSendTool;
import com.neverwinterdp.kafka.tool.server.KafkaCluster;

/**
 * This unit test is used to isolate and show all the kafka producer bugs and limitation. 
 * The following scenarios are tested 
 * @author Tuan
 */

public class KafkaProducerPartitionLeaderKillBugUnitTest  {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/log4j.properties");
  }
  private KafkaCluster cluster ;
  
  @Before
  public void setup() throws Exception {
    cluster = new KafkaCluster("./build/kafka", 1, 2);
    cluster.start();
  }
  
  @After
  public void teardown() throws Exception {
    cluster.shutdown();
    Thread.sleep(3000);
  }
  
  /**
   * This unit test show that the kafka producer loose the messages when while one process send the messages continuosly
   * and another process shutdown the kafka partition leader
   * 
   * REF: http://qnalist.com/questions/5034216/lost-messages-during-leader-election
   * REF: https://issues.apache.org/jira/browse/KAFKA-1211
   * @throws Exception
   */
  @Test
  public void testPartitionKillLeader() throws Exception {
    String TOPIC = "test";
    String[] sendArgs = {
      "--topic" , TOPIC, "--num-partition", "1", "--replication", "2", 
      "--send-writer-type", "default", "--send-period", "0", "--send-max-per-partition", "10000","--send-max-duration", "60000"
    };
    KafkaMessageSendTool sendTool = new KafkaMessageSendTool(sendArgs);
    sendTool.runAsDeamon();;
    
    while(sendTool.getSentCount() <= 0) Thread.sleep(50); //wait to make sure that send tool send some messages
    new KafkaClusterTool(cluster).killLeader(TOPIC, 0 /*partition*/);
    
    sendTool.waitForTermination();
    
    String[] checkArgs = { 
      "--topic", TOPIC,
      "--consume-max", Long.toString(sendTool.getSentCount()),
      "--zk-connect", cluster.getZKConnect()
    };
    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool(checkArgs);
    checkTool.run();
    System.out.println("Sent count = " + sendTool.getReport().getProducerReport().getMessageSent());
    System.out.println("Retried count = " + sendTool.getReport().getProducerReport().getMessageRetried());
    System.out.println("Check count = " + checkTool.getMessageCounter().getTotal());
    checkTool.getMessageTracker().dump(System.out);
    assertTrue(checkTool.getMessageCounter().getTotal() < sendTool.getSentCount());
  }
}