package com.neverwinterdp.scribengin.kafka.source;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaPartitionReaderBak {
  private String name;
  private PartitionMetadata partitionMetadata;
  private String topic; 
  private int    partition;
  private List<String> seedBrokers;
  private List<String> replicaBrokers = new ArrayList<String>();

  public KafkaPartitionReaderBak(String name, List<String> seedBrokers, String topic, int partition) {
    this.name = name;
    this.seedBrokers = seedBrokers;
    this.topic = topic;
    this.partition = partition;
  }
  
  public void run(long maxReads) throws Exception {
    // find the meta data about the topic and partition we are interested in
    PartitionMetadata metadata = findLeader(seedBrokers, topic, partition);
    if (metadata == null) {
      System.out.println("Can't find metadata for Topic and Partition. Exiting");
      return;
    }
    if (metadata.leader() == null) {
      System.out.println("Can't find Leader for Topic and Partition. Exiting");
      return;
    }
    String leadBrokerHost  = metadata.leader().host();
    int    leadBrokerPort = metadata.leader().port();
    String clientName = "Client_" + topic + "_" + partition;

    SimpleConsumer consumer = 
        new SimpleConsumer(leadBrokerHost, leadBrokerPort, 100000, 64 * 1024, clientName);
    long readOffset = getLastOffset(consumer,topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);

    int numErrors = 0;
    while (maxReads > 0) {
      FetchRequest req = 
          new FetchRequestBuilder().
          clientId(clientName).
          addFetch(topic, partition, readOffset, 100000). // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
          build();
      FetchResponse fetchResponse = consumer.fetch(req);

      if (fetchResponse.hasError()) {
        numErrors++;
        // Something went wrong!
        short code = fetchResponse.errorCode(topic, partition);
        System.out.println("Error fetching data from the Broker:" + leadBrokerHost + " Reason: " + code);
        if (numErrors > 5) break;
        if (code == ErrorMapping.OffsetOutOfRangeCode()) {
          // We asked for an invalid offset. For simple case ask for the last element to reset
          readOffset = getLastOffset(consumer,topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
          continue;
        }
        consumer.close();
        consumer = null;
        leadBrokerHost = findNewLeader(leadBrokerHost, topic, partition, leadBrokerPort);
        continue;
      }
      numErrors = 0;

      long numRead = 0;
      for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
        long currentOffset = messageAndOffset.offset();
        if (currentOffset < readOffset) {
          System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
          continue;
        }
        readOffset = messageAndOffset.nextOffset();
        ByteBuffer payload = messageAndOffset.message().payload();

        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);
        System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
        numRead++;
        maxReads--;
      }

      if (numRead == 0) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }
    if (consumer != null) consumer.close();
  }

  private String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {
    for (int i = 0; i < 3; i++) {
      boolean goToSleep = false;
      PartitionMetadata metadata = findLeader(replicaBrokers, topic, partition);
      if (metadata == null) {
        goToSleep = true;
      } else if (metadata.leader() == null) {
        goToSleep = true;
      } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
        // first time through if the leader hasn't changed give ZooKeeper a second to recover
        // second time, assume the broker did recover before failover, or it was a non-Broker issue
        //
        goToSleep = true;
      } else {
        return metadata.leader().host();
      }
      if(goToSleep) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }
    System.out.println("Unable to find new leader after Broker failure. Exiting");
    throw new Exception("Unable to find new leader after Broker failure. Exiting");
  }

  private PartitionMetadata findLeader(List<String> kafkaBrokers, String topic, int partition) {
    PartitionMetadata returnMetaData = null;
    for (String seedBrooker : kafkaBrokers) {
      returnMetaData = findPartitionMetadata(seedBrooker, topic, partition);
      if(returnMetaData != null) break;
    }
    if (returnMetaData != null) {
      replicaBrokers.clear();
      for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
        replicaBrokers.add(replica.host() + ":" + replica.port());
      }
    }
    return returnMetaData;
  }
  
  private PartitionMetadata findPartitionMetadata(String bookerConnect, String topic, int partition) {
    PartitionMetadata returnMetaData = null;
    SimpleConsumer consumer = null;
    int    idx = bookerConnect.lastIndexOf(':') ;
    String host = bookerConnect.substring(0, idx) ;
    int    port = Integer.parseInt(bookerConnect.substring(idx + 1));
    try {
      consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "leaderLookup" /*clientId*/);
      List<String> topics = Collections.singletonList(topic);
      TopicMetadataRequest req = new TopicMetadataRequest(topics);
      TopicMetadataResponse resp = consumer.send(req);

      List<TopicMetadata> metaData = resp.topicsMetadata();
      loop: for (TopicMetadata item : metaData) {
        for (PartitionMetadata part : item.partitionsMetadata()) {
          if (part.partitionId() == partition) {
            returnMetaData = part;
            break loop;
          }
        }
      }
    } catch (Exception e) {
      System.out.println("Error communicating with Broker [" + bookerConnect + "] to find Leader for [" + topic + ", " + partition + "] Reason: " + e);
    } finally {
      if (consumer != null) consumer.close();
    }
    if (returnMetaData != null) {
      replicaBrokers.clear();
      for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
        replicaBrokers.add(replica.host() + ":" + replica.port());
      }
    }
    return returnMetaData;
  }

  static public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
    requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
    OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
      return 0;
    }
    long[] offsets = response.offsets(topic, partition);
    return offsets[0];
  }
}