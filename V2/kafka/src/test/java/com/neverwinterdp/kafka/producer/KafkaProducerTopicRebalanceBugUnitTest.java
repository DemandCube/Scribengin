package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaTool;

public class KafkaProducerTopicRebalanceBugUnitTest extends AbstractBugsUnitTest {

  /*
  * For simple re-balance, we start with two brokers, add a new broker and make it leader
  * 
  * For total re-balance we start with two brokers then move the data to two new brokers.
  * */
  @Override
  public int getKafkaBrokers() {
    return 4;
  }

  //Here we investigate what happens when topic is in brokers 1,2 with broker 1 as leader.
  //We then swap and have broker 2 as leader.
  //Does default producer lose messages?
  
  //Update. This tests fails. This shows that the default kafka writer does not lose messages on swapping leader
 @Test
 @Ignore
  public void testLeaderSwap() throws Exception {

    //create topic on brokers 1,2
    String[] args = { "--create", "--partition", "1", "--replication-factor", "2", "--topic", NAME,
        "--zookeeper", cluster.getZKConnect(), "--replica-assignment", "1:2" };
    KafkaTool tool = new KafkaTool(NAME, cluster.getZKConnect());
    tool.connect();
    tool.createTopic(args);

    // while writing, re-balance topic to have isr {2,1} and have 2 as leader
    DefaultKafkaWriter writer = createKafkaWriter();
    MessageFailDebugCallback failDebugCallback = new MessageFailDebugCallback();
    int[] newBrokers = {2,1};
    int leaderId1 = 0;
    int leaderId2 = 0;
    for (int i = 0; i < NUM_OF_SENT_MESSAGES; i++) {
      writer.send(NAME, 0, "key-" + i, "test-1-" + i, failDebugCallback, 5000);
      if (i == 5) {
        leaderId1 = tool.findPartitionMetadata(NAME, 0).leader().id();
        KafkaLeaderElector leaderElector = new KafkaLeaderElector(NAME, 0, tool, newBrokers);
        new Thread(leaderElector).start();
      }
    }
    writer.close();

    // Thread.sleep(5000);
    String[] checkArgs = { "--topic", NAME,
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
    leaderId2 = tool.findPartitionMetadata(NAME, 0).leader().id();

    System.out.println("initial leader " + leaderId1);
    System.out.println("new leader " + leaderId2);
    //ensure that leader election worked
    assertNotEquals(leaderId1, leaderId2);
    assertEquals(2, leaderId2);

    tool.close();

    System.out.println("send done, failed message count = " + failDebugCallback.failedCount);
    System.out.println("read messages = " + checkTool.getMessageCounter().getTotal());
    assertTrue(checkTool.getMessageCounter().getTotal() < NUM_OF_SENT_MESSAGES);
  }

  /**
   * This unit test show that the kafka producer loses messages when a simple topic rebalance is done.
   * It doesn't have the capability to write to the new leader as the new leader was not among the initial list.
   * 
   * While writing to kafka we move the leader to a broker that wasn't in the initial bootstrap.servers list
   * @throws Exception 
   */
  @Test
  public void testSimpleTopicRebalance() throws Exception {
    //create topic on brokers 2,3
    String[] args = { "--create", "--partition", "1", "--replication-factor", "2", "--topic", NAME,
        "--zookeeper", cluster.getZKConnect(), "--replica-assignment", "2:3" };
    KafkaTool tool = new KafkaTool(NAME, cluster.getZKConnect());
    tool.connect();
    tool.createTopic(args);

    // while writing, re-balance topic to have isr {1,2,3} and have 1 as leader
    DefaultKafkaWriter writer = createKafkaWriter();
    MessageFailDebugCallback failDebugCallback = new MessageFailDebugCallback();
    int[] brokers= {1,2,3};
    int leaderId1 = 0;
    int leaderId2 = 0;
    for (int i = 0; i < NUM_OF_SENT_MESSAGES; i++) {
      writer.send(NAME, 0, "key-" + i, "test-1-" + i, failDebugCallback, 5000);
      if (i == 5) {
        leaderId1 = tool.findPartitionMetadata(NAME, 0).leader().id();
        KafkaLeaderElector leaderElector = new KafkaLeaderElector(NAME, 0, tool, brokers);
        new Thread(leaderElector).start();
      }
    }
    writer.close();

    // Thread.sleep(5000);
    String[] checkArgs = { "--topic", NAME,
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
    leaderId2 = tool.findPartitionMetadata(NAME, 0).leader().id();

    System.out.println("initial leader " + leaderId1);
    System.out.println("new leader " + leaderId2);
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
    //create topic on brokers 1,2
    String[] args = { "--create", "--partition", "1", "--replication-factor", "2", "--topic", NAME,
        "--zookeeper", cluster.getZKConnect(), "--replica-assignment", "1:2" };
    KafkaTool tool = new KafkaTool(NAME, cluster.getZKConnect());
    tool.connect();
    tool.createTopic(args);

    DefaultKafkaWriter writer = createKafkaWriter();

    MessageFailDebugCallback failDebugCallback = new MessageFailDebugCallback();
    int leaderId1 = 0;
    int leaderId2 = 0;

    for (int i = 0; i < NUM_OF_SENT_MESSAGES; i++) {

      writer.send(NAME, 0, "key-" + i, "test-1-" + i, failDebugCallback, 5000);
      if (i == 5) {
        leaderId1 = tool.findPartitionMetadata(NAME, 0).leader().id();
        System.out.println("leader1 " + leaderId1);
        KafkaTopicRebalancer leaderKiller = new KafkaTopicRebalancer(NAME, 0, tool);
        new Thread(leaderKiller).start();
      }
    }
    writer.close();

    String[] checkArgs = { "--topic", NAME,
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

    leaderId2 = tool.findPartitionMetadata(NAME, 0).leader().id();

    System.out.println("initial leader " + leaderId1);
    System.out.println("new leader " + leaderId2);
    //ensure that leader election worked
    assertNotEquals(leaderId1, leaderId2);
    assertEquals(3, leaderId2);

    System.out.println("send done, failed message count = " + failDebugCallback.failedCount);
    System.out.println("read messages = " + checkTool.getMessageCounter().getTotal());
    assertTrue(checkTool.getMessageCounter().getTotal() < NUM_OF_SENT_MESSAGES);
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
      this.tool = tool;
      remainingBrokers = getRemainingBrokers();
    }

    @Override
    public void run() {
      try {
        boolean success = tool.reassignPartition(topic, partition, remainingBrokers);
        logger.info("reasign " + success);
        Thread.sleep(500);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    //a list of broker id's
    //move topic to brokers 3,4
    private List<Object> getRemainingBrokers() {
      List<Object> brokerIds = new LinkedList<Object>();

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
    private int[] brokers;

    public KafkaLeaderElector(String topic, int partition, KafkaTool tool, int[] newBrokers) {
      super();
      this.topic = topic;
      this.partition = partition;
      this.tool = tool;
      this.brokers= newBrokers;
    }

    @Override
    public void run() {
      try {
        KafkaTopicRebalancer topicRebalancer = new KafkaTopicRebalancer(topic, partition, tool);
        topicRebalancer.setNewBrokers(brokers);
        topicRebalancer.run();

        tool.moveLeaderToPreferredReplica(topic, partition);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
