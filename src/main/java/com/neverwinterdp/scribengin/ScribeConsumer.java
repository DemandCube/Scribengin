package com.neverwinterdp.scribengin;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ScribeConsumer {
  private static final Logger LOG = Logger.getLogger(ScribeConsumer.class.getName());

  @Parameter(names = {"-"+Constants.OPT_KAFKA_TOPIC, "--"+Constants.OPT_KAFKA_TOPIC})
  private String topic;

  @Parameter(names = {"-"+Constants.OPT_LEADER, "--"+Constants.OPT_LEADER})
  private HostPort leaderHostPort; // "host:port"

  @Parameter(names = {"-"+Constants.OPT_PARTITION, "--"+Constants.OPT_PARTITION})
  private int partition;

  @Parameter(names = {"-"+Constants.OPT_REPLICA, "--"+Constants.OPT_REPLICA}, variableArity = true)
  private List<String> replicaBrokerList;

  private SimpleConsumer consumer;

  public ScribeConsumer() {
  }

  public void init() {
    //System.out.println("leaderHostPort:" + leaderHostPort); //xxx
    //System.out.println("host: " + leaderHostPort.getHost()); //xxx
    //System.out.println("port: " + leaderHostPort.getPort()); //xxx
    consumer = new SimpleConsumer(
        leaderHostPort.getHost(),
        leaderHostPort.getPort(),
        10000,   // timeout
        64*1024, // buffersize
        getClientName());
  }

  private String getClientName() {
    StringBuilder sb = new StringBuilder();
    sb.append("scribe_");
    sb.append(topic);
    sb.append("_");
    sb.append(partition);
    return sb.toString();
  }

  private long getLastOffset(String topic, int partition, long startTime) {
    TopicAndPartition tp = new TopicAndPartition(topic, partition);

    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(tp, new PartitionOffsetRequestInfo(startTime, 1));

    OffsetRequest req = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), getClientName());

    OffsetResponse resp = consumer.getOffsetsBefore(req);

    if (resp.hasError()) {
      LOG.error("Error when fetching offset: " + resp.errorCode(topic, partition));
      return 0;
    }

    return resp.offsets(topic, partition)[0];
  }

  public void run() {
    long offset = getLastOffset(topic, partition, kafka.api.OffsetRequest.LatestTime());
    System.out.println("offset: " + offset); //xxx
  }

  public static void main(String[] args) {
    System.out.println("wtf");//xxx
    ScribeConsumer sc = new ScribeConsumer();
    JCommander jc = new JCommander(sc);
    jc.addConverterFactory(new CustomConvertFactory());
    jc.parse(args);

    sc.init();
    sc.run();
  }


}
