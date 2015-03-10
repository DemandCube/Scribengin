package com.neverwinterdp.kafka.producer;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import kafka.admin.PreferredReplicaLeaderElectionCommand;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;

import scala.collection.mutable.Set;

import com.neverwinterdp.kafka.tool.KafkaTool;

public class TestUtils {

  // TODO ensure all messages are read
  //TODO check for large messages
  public static List<String> readMessages(String topic, String zkURL) throws Exception {
    int numPartitions = 0;
    List<String> messages = new LinkedList<>();
    KafkaTool tool = new KafkaTool(topic, zkURL);
    tool.connect();

    try {

      waitUntilMetadataIsPropagated(zkURL, topic);
      numPartitions = tool.findTopicMetadata(topic).partitionsMetadata().size();
      //     System.out.println("num partitions "+ numPartitions);
      //   System.out.println("Leader for topic --> " + helper.getLeaderForTopicAndPartition(topic, 0));
      Broker leader;
      for (int i = 0; i < numPartitions; i++) {
        leader = tool.findPartitionMetadata(topic, i).leader();
        try (Consumer consumer = new Consumer(leader.connectionString(), topic, i)) {
          messages.addAll(consumer.read());
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        tool.close();
      } catch (Exception e) {
      }
    }
    //System.out.println("read "+ messages.size() +" messages");
    return messages;
  }

  public static String createRandomTopic() {
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }


  public static void waitUntilMetadataIsPropagated(String zkURL, String topic) {
    ZkClient zkClient = new ZkClient(zkURL, 30000, 30000, ZKStringSerializer$.MODULE$);
    TopicAndPartition partition = new TopicAndPartition(topic, 0);
    Set<TopicAndPartition> x =
        scala.collection.JavaConversions.asScalaSet(Collections.singleton(partition));
    PreferredReplicaLeaderElectionCommand command =
        new PreferredReplicaLeaderElectionCommand(zkClient, x);
    command.moveLeaderToPreferredReplica();
    zkClient.close();
  }
}
