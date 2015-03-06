package com.neverwinterdp.kafka.tool;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.admin.AdminUtils;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.neverwinterdp.kafka.BrokerRegistration;

public class KafkaTool implements Closeable {
  private String name;
  private String zkConnects;
  private ZooKeeper zkClient ;
  private SimpleConsumer consumer ;

  public KafkaTool(String name, String zkConnects) {
    this.name = name;
    this.zkConnects = zkConnects;
  }
  
  public void connect() throws Exception {
    Watcher watcher = new  Watcher() {
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
  
  public ZooKeeper getZookeeper() { return this.zkClient ;}
  
  @Override
  public void close() throws IOException {
    if(consumer != null) {
      consumer.close();
      consumer = null ;
    }
    if(zkClient != null) {
      try {
        zkClient.close();
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
      zkClient = null ;
    }
  }
  
  public void createTopic(String topicName, int numOfReplication, int numPartitions) throws Exception {
    // Create a ZooKeeper client
    int sessionTimeoutMs = 1000;
    int connectionTimeoutMs = 1000;
    ZkClient zkClient = new ZkClient(zkConnects, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    // Create a topic named "myTopic" with 8 partitions and a replication factor of 3
    Properties topicConfig = new Properties();
    AdminUtils.createTopic(zkClient, topicName, numPartitions, numOfReplication, topicConfig);
    Thread.sleep(3000);
    zkClient.close();
  }
  
  /**
   * This delete method doesn't work
   * @param topicName
   * @throws Exception
   */
  @Deprecated
  public void deleteTopic(String topicName) throws Exception {
    int sessionTimeoutMs = 1000;
    int connectionTimeoutMs = 1000;
    ZkClient zkClient = new ZkClient(zkConnects, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    AdminUtils.deleteTopic(zkClient, topicName);
    zkClient.close();
  }
  
  public boolean topicExits(String topicName) throws Exception {
    int sessionTimeoutMs = 1000;
    int connectionTimeoutMs = 1000;
    ZkClient zkClient = new ZkClient(zkConnects, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    boolean exists = AdminUtils.topicExists(zkClient, topicName) ;
    zkClient.close();
    return exists;
  }
  
  public String getKafkaBrokerList() throws KeeperException, InterruptedException  {
    StringBuilder b = new StringBuilder();
    List<BrokerRegistration> registrations = getBrokerRegistration();
    for(int i = 0; i < registrations.size(); i++) {
      BrokerRegistration registration = registrations.get(i);
      if(i > 0) b.append(",");
      b.append(registration.getHost()).append(":").append(registration.getPort());
    }
    return b.toString();
  }
  
  public List<BrokerRegistration> getBrokerRegistration() throws KeeperException, InterruptedException {
    List<String> ids = zkClient.getChildren("/brokers/ids", false) ;
    List<BrokerRegistration> holder = new ArrayList<BrokerRegistration>();
    for(int i = 0; i < ids.size(); i++) {
      String brokerId = ids.get(i);
      BrokerRegistration registration = 
        ZKTool.getDataAs(zkClient, "/brokers/ids/" + brokerId, BrokerRegistration.class);
      registration.setBrokerId(brokerId);
      holder.add(registration);
    }
    return holder;
  }
  
  public TopicMetadata findTopicMetadata(final String topic) throws Exception {
    Operation<TopicMetadata> findTopicOperation = new Operation<TopicMetadata>() {
      @Override
      public TopicMetadata execute() throws Exception {
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> topicMetadatas = resp.topicsMetadata();
        if(topicMetadatas.size() != 1) {
          throw new Exception("Expect to find 1 topic " + topic + ", but found " + topicMetadatas.size());
        }
        return topicMetadatas.get(0);
      }
    };
    return findTopicOperation.execute();
  }

  private void nextLeader() throws Exception {
    List<BrokerRegistration> registrations = getBrokerRegistration();
    if(consumer != null) {
      //Remove the current select broker
      Iterator<BrokerRegistration> i = registrations.iterator();
      while(i.hasNext()) {
        BrokerRegistration sel = i.next();
        if(sel.getHost().equals(consumer.host()) && sel.getPort() == consumer.port()) {
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
  
  static public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
    OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
      return 0;
    }
    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }
  
  static interface Operation<T> {
    public T execute() throws Exception;
  }
}
