package com.neverwinterdp.scribengin.reader.helpers;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;

import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

public class KafkaReaderSimpleConsumer {

  private static final Logger logger = Logger.getLogger(KafkaReaderSimpleConsumer.class);

  private SimpleConsumer consumer;
  private List<Broker> brokerList;
  private String topic;
  private int partition;


  private String clientId;
  private int kafkaPort;
  private int soTimeout;
  private String kafkaHost;
  private int bufferSize;

  public void init(Properties properties) {

    topic = properties.getProperty("kafka.topic");
    partition = Integer.parseInt(properties.getProperty("kafka.partition"));
    kafkaPort = Integer.parseInt(properties.getProperty("kafka.port"));
    kafkaHost = properties.getProperty("kafka.broker.list");
    clientId = Thread.currentThread().getName();
    soTimeout = 10000;
    bufferSize = Integer.parseInt(properties.getProperty("kafka.buffer.size"));

    this.consumer = new SimpleConsumer(kafkaHost, kafkaPort, soTimeout, bufferSize, clientId);

  }

  //TODO get offset from zkhelper. make it generic so we can get it from hazelcast
  public ByteBufferMessageSet read(long offset) {

    FetchRequest req = new FetchRequestBuilder()
        .clientId(getClientName())
        .addFetch(topic, partition, offset, 100000)
        .build();

    FetchResponse resp = consumer.fetch(req);

    if (resp.hasError()) {
      logger.info("has error"); //xxx

      short code = resp.errorCode(topic, partition);
      logger.info("Reason: " + code);
      if (code == ErrorMapping.OffsetOutOfRangeCode()) {
        // We asked for an invalid offset. For simple case ask for the last element to reset
        logger.info("inside errormap");
        offset = getLatestOffsetFromKafka(topic, partition, kafka.api.OffsetRequest.LatestTime());
        //   continue;

        req = new FetchRequestBuilder()
            .clientId(getClientName())
            .addFetch(topic, partition, offset, 100000)
            .build();

        resp = consumer.fetch(req);
      }
    }

    consumer.close();
    logger.info("Offset " + offset + " Messages "
        + Iterators.size(resp.messageSet(topic, partition).iterator()));
    return resp.messageSet(topic, partition);
  }

  private long getLatestOffsetFromKafka(String topic, int partition, long startTime) {
    logger.info("getLatestOffsetFromKafka. topic: " + topic + " partition: " + partition
        + " startTime: " + startTime);
    TopicAndPartition tp = new TopicAndPartition(topic, partition);

    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(tp, new PartitionOffsetRequestInfo(startTime, 1));

    OffsetRequest req = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), getClientName());

    OffsetResponse resp = consumer.getOffsetsBefore(req);


    if (resp.hasError()) {
      logger.info("error when fetching offset: "
          + resp.errorCode(topic, partition));
      logger.info("Is it because you are not talking to the leader? "
          + (resp.errorCode(topic, partition) == ErrorMapping
              .NotLeaderForPartitionCode()));
      logger.info("Is it because we dont have such a topic? "
          + (resp.errorCode(topic, partition) == ErrorMapping
              .UnknownTopicOrPartitionCode()));
      if (resp.errorCode(topic, partition) == ErrorMapping
          .NotLeaderForPartitionCode()) {
        logger.info("Going to look for a new leader.");
        consumer = createNewConsumer();
        resp = consumer.getOffsetsBefore(req);
      }

      return 0;
    }

    return resp.offsets(topic, partition)[0];
  }

  /**
   * Creates a new SimpleConsumer that communicates with the current leader
   * for topic/partition.
   * 
   * Recovers from change of leader or loss of leader.
   * 
   * */
  private SimpleConsumer createNewConsumer() {
    logger.info("createNewConsumer. ");
    Broker leader = null;
    ImmutableList<String> topicList = ImmutableList.of(topic);
    TopicMetadataRequest request = new TopicMetadataRequest(topicList);
    TopicMetadataResponse topicMetadataResponse = null;
    try {
      topicMetadataResponse = consumer.send(request);
    } catch (Exception ce) {
      //broker is down
      if (ce instanceof ConnectException) {
        // ce.printStackTrace();
        //remove that broker from list
        logger.debug("Looping through " + brokerList.size() + " brokers");
        for (int i = 0; i < brokerList.size(); i++) {
          if (brokerList.get(i).host() == consumer.host()
              && brokerList.get(i).port() == consumer.port())
            brokerList.remove(i);
        }
        //connect to another broker
        Broker broker = brokerList.get(0);
        logger.debug("Attempting to connect to  " + broker.port());
        consumer =
            new SimpleConsumer(broker.host(), broker.port(), 1000, 60 * 1024, getClientName());
        topicMetadataResponse = consumer.send(request);
      }
    }

    for (TopicMetadata topicMetadata : topicMetadataResponse
        .topicsMetadata()) {
      logger.info("topic metadata " + topicMetadata.partitionsMetadata().size());
      for (PartitionMetadata partitionMetadata : topicMetadata
          .partitionsMetadata()) {
        logger.info("partition " + partitionMetadata.partitionId());
        if (partitionMetadata.partitionId() == partition) {
          //save these members somewhere
          logger.info("Members " + partitionMetadata.replicas());
          brokerList = partitionMetadata.replicas();
          logger.info("We've found a leader. "
              + partitionMetadata.leader().getConnectionString());
          leader = partitionMetadata.leader();
        }
      }
    }
    logger.info("Leader " + leader);
    SimpleConsumer updatedConsumer = new SimpleConsumer(leader.host(),
        leader.port(), 10000, // timeout
        64 * 1024, // buffer size
        getClientName());
    logger.info("And created a new kafka consumer.");
    return updatedConsumer;
  }

  private String getClientName() {
    return "scribeConsumer_".concat(topic).concat("_").concat(Integer.toString(partition));
  }
}
