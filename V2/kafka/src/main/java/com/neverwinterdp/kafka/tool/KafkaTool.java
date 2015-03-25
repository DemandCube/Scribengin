package com.neverwinterdp.kafka.tool;

import static scala.collection.JavaConversions.asScalaBuffer;
import static scala.collection.JavaConversions.asScalaMap;
import static scala.collection.JavaConversions.asScalaSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import kafka.admin.AdminUtils;
import kafka.admin.PreferredReplicaLeaderElectionCommand;
import kafka.admin.ReassignPartitionsCommand;
import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.common.TopicAndPartition;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import scala.collection.Seq;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.Set;

import com.neverwinterdp.kafka.BrokerRegistration;

public class KafkaTool implements Closeable {
  private String name;
  private String zkConnects;
  private ZooKeeper zkClient;
  private SimpleConsumer consumer;

  public KafkaTool(String name, String zkConnects) {
    this.name = name;
    this.zkConnects = zkConnects;
  }

  public void connect() throws Exception {
    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
      }
    };
    zkClient = new ZooKeeper(zkConnects, 15000, watcher);
    nextLeader();
  }

  public void connect(String zkConnect) throws Exception {
    this.zkConnects = zkConnect;
    connect();
  }

  public ZooKeeper getZookeeper() {
    return this.zkClient;
  }

  @Override
  public void close() throws IOException {
    if (consumer != null) {
      consumer.close();
      consumer = null;
    }
    if (zkClient != null) {
      try {
        zkClient.close();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      zkClient = null;
    }
  }

  public void createTopic(String topicName, int numOfReplication, int numPartitions) throws Exception {
    String[] args = { 
      "--create",
      "--topic", topicName,
      "--partition", String.valueOf(numPartitions),
      "--replication-factor", String.valueOf(numOfReplication),
      "--zookeeper", zkConnects
    };
    createTopic(args);
  }

  /**
   * Create a topic. This method will not create a topic that is currently scheduled for deletion.
   * For valid configs see https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools#Replicationtools-Howtousethetool?.3
   *
   * @See https://kafka.apache.org/documentation.html#topic-config 
   * for more valid configs
   * */
  public void createTopic(String[] args) throws Exception {
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;

    TopicCommandOptions options = new TopicCommandOptions(args);
    ZkClient client = new ZkClient(zkConnects, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    if (topicExits(name)) {
      TopicCommand.deleteTopic(client, options);
    }

    TopicCommand.createTopic(client, options);
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    client.close();
  }

  /**
   * This method works if cluster has "delete.topic.enable" = "true".
   * It can also be implemented by TopicCommand.deleteTopic which simply calls AdminUtils.delete 
   *
   * @param topicName
   * @throws Exception
   */
  public void deleteTopic(String topicName) throws Exception {
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkClient zkClient = new ZkClient(zkConnects, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    AdminUtils.deleteTopic(zkClient, topicName);
    zkClient.close();
  }

  /**
   * Returns true if topic path exists in zk.
   * Warns user if topic is scheduled for deletion.
   * 
   * @See http://search-hadoop.com/m/4TaT4VWNg8/v=plain
   * */
  public boolean topicExits(String topicName) throws Exception {
    int sessionTimeoutMs = 10000;
    int connectionTimeoutMs = 10000;
    ZkClient zkClient = new ZkClient(zkConnects, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    boolean exists = AdminUtils.topicExists(zkClient, topicName);
    if (exists && ZkUtils.pathExists(zkClient, ZkUtils.getDeleteTopicPath(topicName))) {
      System.err.println("Topic "+topicName+" exists but is scheduled for deletion!");
    }
    zkClient.close();
    return exists;
  }

  public String getKafkaBrokerList() throws KeeperException, InterruptedException {
    StringBuilder b = new StringBuilder();
    List<BrokerRegistration> registrations = getBrokerRegistration();
    for (int i = 0; i < registrations.size(); i++) {
      BrokerRegistration registration = registrations.get(i);
      if (i > 0)
        b.append(",");
      b.append(registration.getHost()).append(":").append(registration.getPort());
    }
    return b.toString();
  }

  public List<BrokerRegistration> getBrokerRegistration() throws KeeperException, InterruptedException {
    List<String> ids = zkClient.getChildren("/brokers/ids", false);
    List<BrokerRegistration> holder = new ArrayList<BrokerRegistration>();
    for (int i = 0; i < ids.size(); i++) {
      String brokerId = ids.get(i);
      BrokerRegistration registration =
          ZKTool.getDataAs(zkClient, "/brokers/ids/" + brokerId, BrokerRegistration.class);
      registration.setBrokerId(brokerId);
      holder.add(registration);
    }
    return holder;
  }

  public TopicMetadata findTopicMetadata(final String topic) throws Exception {
    return this.findTopicMetadata(topic, 1);
  }

  public TopicMetadata findTopicMetadata(final String topic, int retries) throws Exception {
    Operation<TopicMetadata> findTopicOperation = new Operation<TopicMetadata>() {
      @Override
      public TopicMetadata execute() throws Exception {
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> topicMetadatas = resp.topicsMetadata();
        if (topicMetadatas.size() != 1) {
          throw new Exception("Expect to find 1 topic " + topic + ", but found " + topicMetadatas.size());
        }

        return topicMetadatas.get(0);
      }
    };
    return execute(findTopicOperation, retries);
  }

  

  public PartitionMetadata findPartitionMetadata(String topic, int partition) throws Exception {
    TopicMetadata topicMetadata = findTopicMetadata(topic);
    for (PartitionMetadata sel : topicMetadata.partitionsMetadata()) {
      if (sel.partitionId() == partition)
        return sel;
    }
    return null;
  }

  private void nextLeader() throws Exception {
    List<BrokerRegistration> registrations = getBrokerRegistration();
    if (consumer != null) {
      //Remove the current select broker
      Iterator<BrokerRegistration> i = registrations.iterator();
      while (i.hasNext()) {
        BrokerRegistration sel = i.next();
        if (sel.getHost().equals(consumer.host()) && sel.getPort() == consumer.port()) {
          i.remove();
          break;
        }
      }
      consumer.close();
      consumer = null;
    }
    Random random = new Random();
    BrokerRegistration registration = registrations.get(random.nextInt(registrations.size()));
    consumer = new SimpleConsumer(registration.getHost(), registration.getPort(), 100000, 64 * 1024, name /*clientId*/);
  }

  // Move Leader to first broker in ISR
  // https://kafka.apache.org/documentation.html#basic_ops_leader_balancing
  //https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools#Replicationtools-2.PreferredReplicaLeaderElectionTool

  public void moveLeaderToPreferredReplica(String topic, int partition) {
    ZkClient client = new ZkClient(zkConnects, 10000, 10000, ZKStringSerializer$.MODULE$);

    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    //move leader to broker 1
    Set<TopicAndPartition> topicsAndPartitions = asScalaSet(Collections.singleton(topicAndPartition));
    PreferredReplicaLeaderElectionCommand commands = new PreferredReplicaLeaderElectionCommand(client, topicsAndPartitions);
    commands.moveLeaderToPreferredReplica();
    client.close();
  }

  /**
   * Re-assign topic/partition to remainingBrokers
   * Remaining brokers is a list of id's of the brokers where the topic/partition is to be moved to.
   * 
   *   Thus if remainingBrokers = [1,2] the topic will be moved to brokers 1 and 2 
   *   
   *   @see https://kafka.apache.org/documentation.html#basic_ops_cluster_expansion
   *   @see https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools#Replicationtools-6.ReassignPartitionsTool
   * */
  public boolean reassignPartition(String topic, int partition, List<Object> remainingBrokers) {
    ZkClient client = new ZkClient(zkConnects, 10000, 10000, ZKStringSerializer$.MODULE$);

    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

    Buffer<Object> seqs = asScalaBuffer(remainingBrokers);
    Map<TopicAndPartition, Seq<Object>> map = new HashMap<>();
    map.put(topicAndPartition, seqs);
    scala.collection.mutable.Map<TopicAndPartition, Seq<Object>> x = asScalaMap(map);
    ReassignPartitionsCommand command = new ReassignPartitionsCommand(client, x);

    return command.reassignPartitions();
  }
  
  public boolean reassignPartitionReplicas(String topic, int partition, Integer ... brokerId) {
    ZkClient client = new ZkClient(zkConnects, 10000, 10000, ZKStringSerializer$.MODULE$);
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

    Buffer<Object> seqs = asScalaBuffer(Arrays.asList((Object[])brokerId));
    Map<TopicAndPartition, Seq<Object>> map = new HashMap<>();
    map.put(topicAndPartition, seqs);
    ReassignPartitionsCommand command = new ReassignPartitionsCommand(client, asScalaMap(map));
    return command.reassignPartitions();
  }

  <T> T execute(Operation<T> op, int retries) throws Exception {
    Exception error = null ;
    for(int i = 0; i < retries; i++) {
      try {
        if(error != null) {
          nextLeader();
        }
        return op.execute();
      } catch(Exception ex) {
        error = ex;
      }
    }
    throw error ;
  }
  
  static interface Operation<T> {
    public T execute() throws Exception;
  }
}
