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
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;

public class ScribenginAM extends AbstractApplicationMaster {

  private static final Logger LOG = Logger.getLogger(ScribenginAM.class.getName());

  @Parameter(names = {"-" + Constants.OPT_KAFKA_SEED_BROKERS, "--" + Constants.OPT_KAFKA_SEED_BROKERS}, variableArity = true)
  private List<HostPort> brokerList; // list of (host:port)s

  @Parameter(names = {"-" + Constants.OPT_KAFKA_TOPIC, "--" + Constants.OPT_KAFKA_TOPIC}, variableArity = true)
  private List<String> topicList;

  @Parameter(names = {"-" + Constants.OPT_CLEAN_START, "--" + Constants.OPT_CLEAN_START})
  private boolean cleanStart=false;
  
  // {topic(String) : { partition(integer) : PartitionMetaData }}
  private Map<String, Map<Integer, PartitionMetadata> > topicMetadataMap;
  
  @Parameter(names = {"-" + Constants.OPT_YARN_SITE_XML, "--" + Constants.OPT_YARN_SITE_XML})
  private static String yarnSiteXml = "/etc/hadoop/conf/yarn-site.xml";
  
  @Parameter(names = {"-"+Constants.OPT_PRE_COMMIT_PATH_PREFIX, "--"+Constants.OPT_PRE_COMMIT_PATH_PREFIX}, description="Pre commit path")
  public String preCommitPrefix="/tmp";
  
  @Parameter(names = {"-"+Constants.OPT_COMMIT_PATH_PREFIX, "--"+Constants.OPT_COMMIT_PATH_PREFIX}, description="Commit path")
  public String commitPrefix="/committed";

  @Parameter(names = {"-"+Constants.OPT_HDFS_PATH, "--"+Constants.OPT_HDFS_PATH}, description="Host:Port of HDFS path")
  public String hdfsPath = null;

  @Parameter(names = {"-"+Constants.OPT_CHECK_POINT_INTERVAL, "--"+Constants.OPT_CHECK_POINT_INTERVAL}, description="Check point interval in milliseconds", required = true)
  public long commitCheckPointInterval = 500; // ms
  
  public ScribenginAM() {
    super(yarnSiteXml);
    topicMetadataMap = new HashMap<String, Map<Integer, PartitionMetadata>>();
  }

  public void init() {
    LOG.info("calling init");
    for (String topic : topicList) {
      getMetaData(topic);
    }
    this.scribeConsumerConfig = new ScribeConsumerConfig();
    scribeConsumerConfig.cleanStart = this.cleanStart;
    scribeConsumerConfig.COMMIT_PATH_PREFIX = this.commitPrefix;
    scribeConsumerConfig.commitCheckPointInterval = this.commitCheckPointInterval;
    scribeConsumerConfig.PRE_COMMIT_PATH_PREFIX = this.preCommitPrefix;
    scribeConsumerConfig.hdfsPath = this.hdfsPath;
    
    //scribeConsumerConfig.applicationMasterMem = this.applicationMasterMem;
    //scribeConsumerConfig.appMasterClassName = this.applicationMasterClassName;
    //scribeConsumerConfig.appname = this.appname;
    //scribeConsumerConfig.brokerList = this.brokerList;
    //scribeConsumerConfig.containerMem = this.containerMem;
    //scribeConsumerConfig.defaultFs = this.defaultFs;
    //scribeConsumerConfig.partition = this.partition;
    //scribeConsumerConfig.yarnSiteXml = yarnSiteXml;
    //scribeConsumerConfig.topic
    //scribeConsumerConfig.scribenginJarPath =
  }
  
  @Override
  protected List<String> buildCommandList(ScribeConsumerConfig c) {
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
        
        sb.append("-cp scribeconsumer.jar " + com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumer.class.getName())
            .append(" --"+Constants.OPT_BROKER_LIST+" "+getBrokerListStr())
            .append(" --"+Constants.OPT_CHECK_POINT_INTERVAL+" "+Long.toString(c.commitCheckPointInterval))
            .append(" --"+Constants.OPT_COMMIT_PATH_PREFIX+" "+c.COMMIT_PATH_PREFIX)
            .append(" --"+Constants.OPT_PARTITION+" "+ Integer.toString(partition))
            .append(" --"+Constants.OPT_PRE_COMMIT_PATH_PREFIX+" "+ c.PRE_COMMIT_PATH_PREFIX)
            .append(" --"+Constants.OPT_KAFKA_TOPIC+" "+t)
            ;
         if(c.hdfsPath != null){
           sb.append(" --"+Constants.OPT_HDFS_PATH+" "+c.hdfsPath);
         }
         if(c.cleanStart){
           sb.append(" --"+Constants.OPT_CLEAN_START);
         }
        
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
