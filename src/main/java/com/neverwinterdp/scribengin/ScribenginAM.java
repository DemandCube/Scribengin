package com.neverwinterdp.scribengin;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import com.beust.jcommander.Parameter;


public class ScribenginAM {

  //@Parameter(names = {"-" + Constants.OPT_KAFKA_TOPIC, "--" + Constants.OPT_KAFKA_TOPIC})
  //private String topic;

  @Parameter(names = {"-" + Constants.OPT_KAFKA_SEED_BROKERS, "--" + Constants.OPT_KAFKA_SEED_BROKERS}, variableArity = true)
  private List<String> kafkaSeedBrokers;

  @Parameter(names = {"-" + Constants.OPT_KAFKA_PORT, "--" + Constants.OPT_KAFKA_PORT})
  private int port;

  // TODO: This is not ideal.
  // topic is repeated in topicList and topicMetadatMap. However, with jcommander automatically parses and store
  // parsed cli arguments to member variables, this is the best I can come up with right now
  @Parameter(names = {"-" + Constants.OPT_KAFKA_TOPIC, "--" + Constants.OPT_KAFKA_TOPIC}, variableArity = true)
  private List<String> topicList;
  // {topic(String) : { partition(integer) : PartitionMetaData }}
  private Map<String, Map<Integer, PartitionMetadata> > topicMetadataMap;


  public ScribenginAM() {
    super();
    topicMetadataMap = new HashMap<String, Map<Integer, PartitionMetadata>>();
  }

  public void init(String[] args) {
    for (String topic : topicList) {
      getMetaData(kafkaSeedBrokers, port, topic);
    }
  }

  //TODO: pass the following to each container
  // 1) partition number
  // 2) topic name
  // 3) replica host
  //

  private void getMetaData(List<String> seedBrokerList, int port, String topic) {
    for (String seed: seedBrokerList) {
      SimpleConsumer consumer = new SimpleConsumer(
          seed,
          port,
          10000,   // timeout
          64*1024, // bufgerSize
          "metaLookup"  // clientId
          );
      List <String> topicList = Collections.singletonList(topic);

      TopicMetadataRequest req = new TopicMetadataRequest(topicList);
      kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
      List<TopicMetadata> metaDataList = resp.topicsMetadata();
      for (TopicMetadata m: metaDataList) {
        for (PartitionMetadata part : m.partitionsMetadata()) {
          storeMetadata(topic, part);
        }
      }
    }
  }

  private void storeMetadata(String topic, PartitionMetadata p) {
    Integer id = new Integer(p.partitionId());
    Map<Integer, PartitionMetadata> m;

    if (topicMetadataMap.containsKey(id)) {
      m = topicMetadataMap.get(topic);
    } else {
      m = new HashMap<Integer, PartitionMetadata>();
      topicMetadataMap.put(topic, m);
    }

    m.put(id, p);
  }
}
