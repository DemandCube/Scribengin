package com.neverwinterdp.scribengin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class ScribenginAM extends AbstractApplicationMaster {
  // hadoop jar target/scribengin-uber-0.0.1-SNAPSHOT.jar com.neverwinterdp.scribengin.Client -am_mem 300 -container_mem 300 --container_cnt 4 --hdfsjar /scribengin-uber-0.0.1-SNAPSHOT.jar --app_name scribe --command "echo" --am_class_name "com.neverwinterdp.scribengin.ScribenginAM" -topic "scribe" -kafka_seed_brokers "10.0.2.15" -kafka_port 9092

  private static final Logger LOG = Logger.getLogger(ScribenginAM.class.getName());

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
    super.init(args);
    LOG.info("calling init");
    for (String topic : topicList) {
      getMetaData(kafkaSeedBrokers, port, topic);
    }
  }

  @Override
  protected List<String> buildCommandList(int startingFrom, int containerCnt, String commandTemplate) {
    // TODO: construnct the list of actual commands for containers to execute.
    // A container should be able to read from more than one partition.
    List<String> r = new ArrayList<String>();
    for ( Map.Entry<String, Map<Integer, PartitionMetadata> > entry : topicMetadataMap.entrySet() ) {
      String t = entry.getKey();
      LOG.info("topic : " + t);

      for ( Map.Entry<Integer, PartitionMetadata> innerEntry: entry.getValue().entrySet()) {
        Integer partition = innerEntry.getKey();
        PartitionMetadata meta = innerEntry.getValue();
        LOG.info("\tpartition: " + partition);
        LOG.info("\t\t leader: " + meta.leader());
      }
    }
    return r;
  }

  //TODO: pass the following to each container
  // 1) partition number
  // 2) topic name
  // 3) replica host
  //

  private void getMetaData(List<String> seedBrokerList, int port, String topic) {
    LOG.info("inside getMetaData"); //xxx
    LOG.info("seedBrokerList size: " + seedBrokerList); //xxx

    for (String seed: seedBrokerList) {
      LOG.info("making a simple consumer"); //xxx
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
      LOG.info("metaDataList: " + metaDataList); //xxxx

      for (TopicMetadata m: metaDataList) {
        LOG.info("inside the metadatalist loop"); //xxx
        LOG.info("m partitionsMetadata: " + m.partitionsMetadata()); //xxx
        for (PartitionMetadata part : m.partitionsMetadata()) {
          LOG.info("inside the partitionmetadata loop"); //xxx
          storeMetadata(topic, part);
        }
      }
    }
  }

  private void storeMetadata(String topic, PartitionMetadata p) {
    Integer id = new Integer(p.partitionId());
    Map<Integer, PartitionMetadata> m;

    if (topicMetadataMap.containsKey(id)) {
      LOG.info("already crreated a partitionMap. Just retrieve it."); //xxx
      m = topicMetadataMap.get(topic);
    } else {
      LOG.info("making a new partitionMap"); //xxx
      m = new HashMap<Integer, PartitionMetadata>();
      topicMetadataMap.put(topic, m);
    }

    m.put(id, p);
  }

  public static void main(String[] args) {
    AbstractApplicationMaster am = new ScribenginAM();
    new JCommander(am, args);
    am.init(args);

    LOG.info("calling main");

    try {
      am.run();
    } catch (Exception e) {
      System.out.println("am.run throws: " + e);
      e.printStackTrace();
      System.exit(0);
    }
  }
}
