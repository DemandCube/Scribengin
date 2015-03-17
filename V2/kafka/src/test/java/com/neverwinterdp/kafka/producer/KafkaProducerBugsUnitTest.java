package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.kafka.KafkaCluster;

/**
 * This unit test is used to isolate and show all the kafka producer bugs and limitation.
 * The following scenarios are tested 
 * 
 * @author Tuan
 */

public class KafkaProducerBugsUnitTest {

  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties");
  }

  private static String NAME = "test";
  private static int NUM_OF_SENT_MESSAGES = 50000;

  private Logger logger = Logger.getLogger(getClass());
  private KafkaCluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = new KafkaCluster("./build/kafka", 1, 4);
    cluster.setReplication(2);
    cluster.setNumOfPartition(1);
    cluster.start();
    Thread.sleep(2000);
  }

  @After
  public void tearDown() throws Exception {
    logger.info("from here we shutdown");
    cluster.shutdown();
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
  public void kafkaProducerLooseMessageWhenThePartitionLeaderShutdown() throws Exception {
    DefaultKafkaWriter writer = createKafkaWriter();
    MessageFailDebugCallback failDebugCallback = new MessageFailDebugCallback();
    for (int i = 0; i < NUM_OF_SENT_MESSAGES; i++) {

      //Use this send to print out more detail about the message lost
      writer.send("test", 0, "key-" + i, "test-1-" + i, failDebugCallback, 5000);
      //writer.send("test", 0, "key-" + i, "test-1-" + i, 5000);
      //After sending 10 messages we shutdown and continue sending
      if (i == 10) {
        KafkapartitionLeaderKiller leaderKiller = new KafkapartitionLeaderKiller("test", 0);
        new Thread(leaderKiller).start();
        //IF we use the same writer thread to shutdown the leader and resume the sending. No message are lost
        //leaderKiller.run();
      }
    }
    writer.close();
    System.out.println("send done, failed message count = " + failDebugCallback.failedCount);
    String[] checkArgs = {"--topic", "test",
        "--consume-max", Integer.toString(NUM_OF_SENT_MESSAGES),
        "--zk-connect", cluster.getZKConnect(),
      };
    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool();
    new JCommander(checkTool, checkArgs);
    //KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool(cluster.getZKConnect(), "test", NUM_OF_SENT_MESSAGES);
    checkTool.runAsDeamon();
    if (checkTool.waitForTermination(10000)) {
      checkTool.setInterrupt(true);
      Thread.sleep(3000);
    }
    assertTrue(checkTool.getMessageCounter().getTotal() < NUM_OF_SENT_MESSAGES);
  }

  /**
   * This unit test show that the kafka producer loses messages when a simple topic rebalance is done.
   * It doesn't have the capability to write to the new leader as the new leader was not among the initial list.
   * 
   * While writing to kafka we move the leader to a broker that wasnt in the initial bootstrap.servers list
   * @throws Exception 
   */
  @Test
  public void testSimpleTopicRebalance() throws Exception {
    //create topic on brokers 2,3
    String[] args = { "--create", "--partition", "1", "--replication-factor", "2", "--topic", NAME,
        "--zookeeper", "127.0.0.1:2181", "--replica-assignment", "2:3" };
    KafkaTool tool = new KafkaTool(NAME, cluster.getZKConnect());
    tool.connect();
    tool.createTopic(args);
   
    // while writing, rebalance topic to have isr {1,2,3} and have 1 as leader
    DefaultKafkaWriter writer = createKafkaWriter();
    MessageFailDebugCallback failDebugCallback = new MessageFailDebugCallback();
    int leaderId1=0;
    int leaderId2=0;
    for (int i = 0; i < NUM_OF_SENT_MESSAGES; i++) {
      writer.send("test", 0, "key-" + i, "test-1-" + i, failDebugCallback, 5000);
      if (i == 5) {
        leaderId1 = tool.findPartitionMetadata(NAME, 0).leader().id();
        KafkaLeaderElector leaderElector = new KafkaLeaderElector("test", 0, tool);
        new Thread(leaderElector).start();
      }
    }
    writer.close();

    // Thread.sleep(5000);
    String[] checkArgs = {"--topic", "test",
        "--consume-max", Integer.toString(NUM_OF_SENT_MESSAGES),
        "--zk-connect", cluster.getZKConnect(),
      };
    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool();
    new JCommander(checkTool, checkArgs);

    checkTool.runAsDeamon();
    if (checkTool.waitForTermination(10000)) {
      checkTool.setInterrupt(true);
      Thread.sleep(3000);
    }
    leaderId2= tool.findPartitionMetadata(NAME, 0).leader().id();

    
    System.out.println("initial leader "+ leaderId1);
    System.out.println("new leader "+ leaderId2);
    //ensure that leader election worked
    assertNotEquals(leaderId1, leaderId2);
    assertEquals(1, leaderId2);

    tool.close();

    System.out.println("send done, failed message count = " + failDebugCallback.failedCount);
    System.out.println("read messages = " + checkTool.getMessageCounter().getTotal());
    assertTrue(checkTool.getMessageCounter().getTotal() < NUM_OF_SENT_MESSAGES);
  }

  /**
   * This unit test show that the kafka producer loses messages when topic is rebalanced to new brokers.
   * It doesn't have the capability to 'know' the new leader as the new leader was not among the initial list.
   * 
   * While writing to kafka we move the topic/partition to new brokers
   */
  @Test
  public void testTotalTopicRebalance() throws Exception {
    DefaultKafkaWriter writer = createKafkaWriter();
    KafkaTool tool = new KafkaTool(NAME, cluster.getZKConnect());
    tool.connect();
    MessageFailDebugCallback failDebugCallback = new MessageFailDebugCallback();
    for (int i = 0; i < NUM_OF_SENT_MESSAGES; i++) {

      writer.send("test", 0, "key-" + i, "test-1-" + i, failDebugCallback, 5000);
      if (i == 5) {
        KafkaTopicRebalancer leaderKiller = new KafkaTopicRebalancer("test", 0, tool);
        new Thread(leaderKiller).start();
      }
    }
    writer.close();

    String[] checkArgs = {"--topic", "test",
        "--consume-max", Integer.toString(NUM_OF_SENT_MESSAGES),
        "--zk-connect", cluster.getZKConnect(),
      };
    KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool();
    new JCommander(checkTool, checkArgs);
    //KafkaMessageCheckTool checkTool = new KafkaMessageCheckTool(cluster.getZKConnect(), "test", NUM_OF_SENT_MESSAGES);
    checkTool.runAsDeamon();
    if (checkTool.waitForTermination(10000)) {
      checkTool.setInterrupt(true);
      Thread.sleep(3000);
    }
    tool.close();
    System.out.println("send done, failed message count = " + failDebugCallback.failedCount);
    System.out.println("read messages = " + checkTool.getMessageCounter().getTotal());
    Assert.assertTrue(checkTool.getMessageCounter().getTotal() < NUM_OF_SENT_MESSAGES);
  }

  private DefaultKafkaWriter createKafkaWriter() {
    Map<String, String> kafkaProps = new HashMap<String, String>();
    kafkaProps.put("message.send.max.retries", "5");
    kafkaProps.put("retry.backoff.ms", "100");
    kafkaProps.put("queue.buffering.max.ms", "1000");
    kafkaProps.put("queue.buffering.max.messages", "15000");
    //kafkaProps.put("request.required.acks", "-1");
    kafkaProps.put("topic.metadata.refresh.interval.ms", "-1"); //negative value will refresh on failure
    kafkaProps.put("batch.num.messages", "100");
    kafkaProps.put("producer.type", "sync");
    //new config:
    kafkaProps.put("acks", "all");
    DefaultKafkaWriter writer = new DefaultKafkaWriter(NAME, kafkaProps, cluster.getKafkaConnect());
    logger.info("we have created a writer");
    return writer;
  }

  class MessageFailDebugCallback implements Callback {
    private int failedCount;

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null)
        failedCount++;
    }
  }

  class KafkapartitionLeaderKiller implements Runnable {
    private String topic;
    private int partition;

    KafkapartitionLeaderKiller(String topic, int partition) {
      this.topic = topic;
      this.partition = partition;
    }

    public void run() {
      try {
        KafkaTool kafkaTool = new KafkaTool("test", cluster.getZKConnect());
        kafkaTool.connect();
        TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topic);
        PartitionMetadata partitionMeta = findPartition(topicMeta, partition);
        Broker partitionLeader = partitionMeta.leader();
        Server kafkaServer = cluster.findKafkaServerByPort(partitionLeader.port());
        System.out.println("Shutdown kafka server " + kafkaServer.getPort());
        kafkaServer.shutdown();
        kafkaTool.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    PartitionMetadata findPartition(TopicMetadata topicMetadata, int partion) {
      for (PartitionMetadata sel : topicMetadata.partitionsMetadata()) {
        if (sel.partitionId() == partition)
          return sel;
      }
      throw new RuntimeException("Cannot find the partition " + partition);
    }
  }

  class KafkaTopicRebalancer implements Runnable {

    private int partition;
    private String topic;
    List<Object> remainingBrokers;
    private KafkaTool tool;

    public KafkaTopicRebalancer(String topic, int partition, KafkaTool tool) {
      super();
      this.topic = topic;
      this.partition = partition;
      this.tool= tool;
      remainingBrokers = getRemainingBrokers();
      logger.info("hahahahah");
    }

    @Override
    public void run() {
      try {
        System.out.println("reasign " + tool.reassignPartition(topic, partition, remainingBrokers));
        Thread.sleep(500);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    //a list of broker id's
    private List<Object> getRemainingBrokers() {
      List<Object> brokerIds = new ArrayList<Object>();

      brokerIds.add(3);
      brokerIds.add(4);

      return brokerIds;
    }

    public void setNewBrokers(int... i) {
      remainingBrokers.clear();

      for (int j = 0; j < i.length; j++) {
        remainingBrokers.add(i[j]);
      }
      System.out.println("remaining brokers");
    }
  }

  class KafkaLeaderElector implements Runnable {

    private int partition;
    private String topic;
    private KafkaTool tool;

    public KafkaLeaderElector(String topic, int partition, KafkaTool tool) {
      super();
      this.topic = topic;
      this.partition = partition;
      this.tool=tool;
     }

    @Override
    public void run() {
      try {
        KafkaTopicRebalancer topicRebalancer = new KafkaTopicRebalancer(topic, partition, tool);
        topicRebalancer.setNewBrokers(1, 2, 3);
        topicRebalancer.run();

        tool.moveLeaderToPreferredReplica(topic, partition);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}