package com.neverwinterdp.kafka.producer;

import static org.junit.Assert.assertTrue;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.kafka.tool.KafkaMessageCheckTool;
import com.neverwinterdp.kafka.tool.KafkaTool;
import com.neverwinterdp.server.Server;

/**
 * This unit test is used to isolate and show all the kafka producer bugs and limitation.
 * The following scenarios are tested 
 * 
 * @author Tuan
 */

public class KafkaProducerBugsUnitTest extends AbstractBugsUnitTest {

  @Override
  public int getKafkaBrokers() {
    return 2;
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
    String[] checkArgs = { "--topic", "test",
        "--consume-max", Integer.toString(NUM_OF_SENT_MESSAGES),
        "--zk-connect", cluster.getZKConnect()
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

}