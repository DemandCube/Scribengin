package com.neverwinterdp.kafka.tool;

import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import com.neverwinterdp.tool.server.Server;
import com.neverwinterdp.kafka.tool.server.KafkaCluster;

public class KafkaClusterTool {
  private KafkaCluster cluster;
  
  public KafkaClusterTool(KafkaCluster cluster) {
    this.cluster = cluster;
  }
  
  public void killLeader(String topic, int partition) throws Exception {
    findLeader(topic, partition).shutdown();
  }
  
  public void restartLeader(String topic, int partition) throws Exception {
    Server server = findLeader(topic, partition);
    server.shutdown();
    server.start();
  }
  
  Server findLeader(String topic, int partition) throws Exception {
    KafkaTool kafkaTool = new KafkaTool("KafkaPartitionLeaderKiller", cluster.getZKConnect());
    kafkaTool.connect();
    TopicMetadata topicMeta = kafkaTool.findTopicMetadata(topic);
    PartitionMetadata partitionMeta = findPartition(topicMeta, partition);
    Broker partitionLeader = partitionMeta.leader();
    Server kafkaServer = cluster.findKafkaServerByPort(partitionLeader.port());
    System.out.println("Shutdown kafka server " + kafkaServer.getPort());
    kafkaTool.close();
    return kafkaServer;
  }
  
  PartitionMetadata findPartition(TopicMetadata topicMetadata, int partition) {
    for (PartitionMetadata sel : topicMetadata.partitionsMetadata()) {
      if (sel.partitionId() == partition)
        return sel;
    }
    throw new RuntimeException("Cannot find the partition " + partition);
  }
}
