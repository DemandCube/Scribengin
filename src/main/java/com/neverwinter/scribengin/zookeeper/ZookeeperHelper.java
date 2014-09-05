package com.neverwinter.scribengin.zookeeper;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;

import com.google.common.collect.Multimap;
import com.neverwinter.scribengin.utils.ConfigurationCommand;
import com.neverwinter.scribengin.utils.HostPort;
import com.neverwinter.scribengin.utils.Partition;
import com.neverwinter.scribengin.utils.PartitionState;
import com.neverwinter.scribengin.utils.ScribenginUtils;

public class ZookeeperHelper {

  private String zkConnectString;
  private CuratorFramework zkClient;
  private final static RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

  private static final Logger logger = Logger
      .getLogger(ZookeeperHelper.class);
  private String brokerInfoLocation = "/brokers/ids/";
  private String topicInfoLocation = "/brokers/topics/";

  public ZookeeperHelper(String zookeeperURL) throws InterruptedException {
    super();
    zkConnectString = zookeeperURL;
    zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        retryPolicy);
    init();
  }

  private void init() throws InterruptedException {
    zkClient.start();
    zkClient.blockUntilConnected();
  }

  public HostPort getLeader(String topic, int partion) throws Exception {

    String[] values = getLeaderForTopicAndPartition(topic, partion).split(
        ":");
    if (values.length == 2)
      return new HostPort(values[0], values[1]);
    else
      return null;
  }

  public String getLeaderForTopicAndPartition(String topic, int partion)
      throws Exception {
    String leader = "";

    com.neverwinter.scribengin.utils.PartitionState partitionState = getPartionState(topic, partion);
    int leaderId = partitionState.getLeader();
    byte[] bytes = {};

    try {
      if (leaderId == -1) {
        return leader;
      }
      logger.debug("Going to look for " + brokerInfoLocation + leaderId);
      bytes = zkClient.getData().forPath(brokerInfoLocation + leaderId);
    }

    catch (NoNodeException nne) {
      logger.error(nne.getMessage(), nne);
      return leader;
    }
    Partition part = ScribenginUtils.toClass(bytes, Partition.class);
    logger.debug("leader " + part);

    return leader.concat(part.getHost()).concat(":")
        .concat(String.valueOf(part.getPort()));

  }

  /* /brokers/[0...N] --> { "host" : "host:port",
  	                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
  	                                    "topicN": ["partition1" ... "partitionN"] } }*/
  public List<String> getBrokersForTopicAndPartition(String topic, int partion)
      throws Exception {
    PartitionState partitionState = getPartionState(topic, partion);
    StringBuilder broker = new StringBuilder();
    List<String> brokers = new LinkedList<String>();
    byte[] partitions;
    logger.debug("PartitionState " + partitionState);

    for (Integer b : partitionState.getIsr()) {
      // TODO broker is registered but offline next line throws
      // nonodeexception
      try {
        partitions = zkClient.getData().forPath(brokerInfoLocation + b);
        Partition part = ScribenginUtils.toClass(partitions, Partition.class);
        broker.append(part.getHost());
        broker.append(":");
        broker.append(part.getPort());
        brokers.add(broker.toString());
        broker.setLength(0);
      } catch (NoNodeException nne) {
        logger.debug(nne.getMessage());
      }
    }
    return brokers;
  }

  public Map<String, String> getData(String path) throws Exception {
    System.err.println("getData. path: " + path);
    if (zkClient.checkExists().forPath(path) == null) {
      return Collections.emptyMap();
    }

    byte[] data = zkClient.getData().forPath(path);

    return ScribenginUtils.toMap(data);

  }

  private boolean checkPathExists(String config) throws Exception {
    return zkClient.checkExists().forPath(config) != null;

  }

  public int writeData(String path, byte[] data) throws Exception {
    //TODO exit if data is not a json obj

    logger.info("writeData. path: " + path + " data: " + Arrays.toString(data));
    if (zkClient.checkExists().forPath(path) == null) {

      String created =
          zkClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
              .forPath(path);
      logger.debug("what happened " + created);
    }
    Stat stat = zkClient.setData().forPath(path, data);

    return stat.getDataLength();
  }

  // TODO multimap for all topics

  public Multimap<String, String> getBrokersForAllTopics() {
    return null;

  }

  /**
   * @param topic
   * @param partion
   * @return
   * @throws Exception
   */
  private PartitionState getPartionState(String topic, int partion)
      throws Exception {

    try {
      zkClient.getData().forPath(topicInfoLocation + topic);
    } catch (NoNodeException nne) {
      // there are no nodes for the topic. We return an empty
      // Partitionstate
      logger.error(nne.getMessage());
      return new PartitionState();
    }
    byte[] bytes = zkClient.getData()
        .forPath(
            topicInfoLocation + topic + "/partitions/" + partion
                + "/state");
    PartitionState partitionState = ScribenginUtils.toClass(bytes,
        PartitionState.class);
    return partitionState;
  }

  // where is the info
  // brokers/id and /brokers/topics
  // to be used only when kafka broker info is not stored in default zookeeper
  // location
  public void setBrokerInfoLocation(String brokerInfoLocation) {
    this.brokerInfoLocation = brokerInfoLocation;
  }

  public void setTopicInfoLocation(String topicInfoLocation) {
    this.topicInfoLocation = topicInfoLocation;
  }

  public static void main(String[] args) {

    BasicConfigurator.configure();
    String config = "/scribengin/config";

    ConfigurationCommand command = new ConfigurationCommand();
    command.setMemberName("127.0.0.6");
    command.setObeyed(false);
    try {
      ZookeeperHelper helper = new ZookeeperHelper("192.168.33.33:2181");

      System.err.println(helper.checkPathExists(config));
      System.err.println(helper.writeData(config, ScribenginUtils.toJson(command).getBytes()));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  //write to 
  public void updateProgress(String path, byte[] data) throws Exception {
    writeData(path, data);
  }
}
