package com.neverwinterdp.scribengin.yarn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.constants.Constants;
import com.neverwinterdp.scribengin.hostport.CustomConvertFactory;
import com.neverwinterdp.scribengin.hostport.HostPort;


public class ScribenginAM extends AbstractApplicationMaster {
  // /hadoop/yarn/local is where the local dir is on the local filesystem.
  // yarn application -kill [application id]
  // copy the jar file to hdfs. I copy scribengin-1.0-SNAPSHOT.jar to hdfs's /
  // hadoop jar scribengin-1.0-SNAPSHOT.jar -am_mem 300 -container_mem 300 --container_cnt 4 --hdfsjar /scribengin-1.0-SNAPSHOT.jar --app_name scribe --command "echo" --am_class_name "com.neverwinterdp.scribengin.ScribenginAM" -topic "scribe" -kafka_seed_brokers "10.0.2.15" -kafka_port 9092

  private static final Logger LOG = Logger.getLogger(ScribenginAM.class.getName());

  @Parameter(names = {"-" + Constants.OPT_KAFKA_SEED_BROKERS, "--" + Constants.OPT_KAFKA_SEED_BROKERS}, variableArity = true)
  private List<HostPort> brokerList; // list of (host:port)s

  // TODO: This is not ideal.
  // topic is repeated in topicList and topicMetadataMap. However, with jcommander automatically parses and store
  // parsed cli arguments to member variables, this is the best I can come up with right now
  @Parameter(names = {"-" + Constants.OPT_KAFKA_TOPIC, "--" + Constants.OPT_KAFKA_TOPIC}, variableArity = true)
    private List<String> topicList;

  // {topic(String) : { partition(integer) : PartitionMetaData }}
  private Map<String, Map<Integer, PartitionMetadata> > topicMetadataMap;


  public ScribenginAM() {
    super();
    topicMetadataMap = new HashMap<String, Map<Integer, PartitionMetadata>>();
  }

  public void init() {
    LOG.info("calling init");
    for (String topic : topicList) {
      getMetaData(topic);
    }
  }

  @Override
  protected List<String> buildCommandList(int startingFrom, int containerCnt) {
    LOG.info("buildCommandList. ");
    List<String> r = new ArrayList<String>();
    for ( Map.Entry<String, Map<Integer, PartitionMetadata> > entry : topicMetadataMap.entrySet() ) {
      String t = entry.getKey();
      LOG.info("topic : " + t);

      for ( Map.Entry<Integer, PartitionMetadata> innerEntry: entry.getValue().entrySet()) {
        Integer partition = innerEntry.getKey();
        LOG.info("partition: " + partition);

        StringBuilder sb = new StringBuilder();
        sb.append(Environment.JAVA_HOME.$()).append("/bin/java").append(" ");
        sb.append("-cp scribeconsumer.jar com.neverwinterdp.scribengin.ScribeConsumer --topic scribe --checkpoint_interval 100 --broker_list ");
        sb.append(getBrokerListStr());
        sb.append(" --partition ");
        sb.append(Integer.toString(partition));
        r.add(sb.toString());
      }
    }
    LOG.info("Command list "+ r);
    return r;
  }

  private String getBrokerListStr() {
    StringBuilder sb = new StringBuilder();
    int len = brokerList.size();
    for (int i = 0; i < len; i++) {
      sb.append(brokerList.get(i).toString());
      if ((i + 1) < len)
        sb.append(",");
    }
    return sb.toString();
  }

  private void getMetaData(String topic) {
    LOG.info("inside getMetaData"); //xxx
    LOG.info("seedBrokerList" + this.brokerList); //xxx

    for (HostPort seed: brokerList) {
      SimpleConsumer consumer = new SimpleConsumer(
          seed.getHost(),
          seed.getPort(),
          10000,   // timeout
          64*1024, // bufferSize
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
    JCommander jc = new JCommander(am);
    jc.addConverterFactory(new CustomConvertFactory());
    jc.parse(args);

    LOG.info("calling main");

    try {
      am.init();
      am.run();
    } catch (Exception e) {
      System.out.println("am.run throws: " + e);
      e.printStackTrace();
      System.exit(0);
    }
  }
}
