package com.neverwinterdp.scribengin;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ScribeConsumer {
  // Random comments:
  // Unique define a partition. Client name + topic name + offset
  // java -cp scribengin-uber-0.0.1-SNAPSHOT.jar com.neverwinterdp.scribengin.ScribeConsumer --topic scribe  --leader 10.0.2.15:9092 --checkpoint_interval 100 --partition 1
  // checkout src/main/java/com/neverwinterdp/scribengin/ScribeConsumer.java
  // checkout org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
  // checkout EtlMultiOutputCommitter in Camus

  private static final Logger LOG = Logger.getLogger(ScribeConsumer.class.getName());

  @Parameter(names = {"-"+Constants.OPT_KAFKA_TOPIC, "--"+Constants.OPT_KAFKA_TOPIC})
  private String topic;

  @Parameter(names = {"-"+Constants.OPT_LEADER, "--"+Constants.OPT_LEADER})
  private HostPort leaderHostPort; // "host:port"

  @Parameter(names = {"-"+Constants.OPT_PARTITION, "--"+Constants.OPT_PARTITION})
  private int partition;

  @Parameter(names = {"-"+Constants.OPT_REPLICA, "--"+Constants.OPT_REPLICA}, variableArity = true)
  private List<String> replicaBrokerList;

  @Parameter(names = {"-"+Constants.OPT_CHECK_POINT_TIMER, "--"+Constants.OPT_CHECK_POINT_TIMER}, description="Check point interval in milliseconds")
  private long commitCheckPointInterval; // ms

  private SimpleConsumer consumer;
  private Timer checkPointIntervalTimer;

  public ScribeConsumer() {
    checkPointIntervalTimer = new Timer();
  }

  public void init() throws IOException {
    consumer = new SimpleConsumer(
        leaderHostPort.getHost(),
        leaderHostPort.getPort(),
        10000,   // timeout
        64*1024, // buffersize
        getClientName());

    scheduleCommitTimer();
  }

  private void scheduleCommitTimer() {
    checkPointIntervalTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        commit();
      }
    }, commitCheckPointInterval);
  }

  private void commit() {
    //TODO: move from tmp to the actual partition.
    // TODO: synchronize with the writing code in run()
    System.out.println(">> committing");

    //Close out the old writer
    //writer.close();
    //try {
      //writer = new StringRecordWriter("/tmp/scribe_data");
    //} catch (IOException e) {
      ////exit early.
    //}

    scheduleCommitTimer();
  }

  private String getClientName() {
    StringBuilder sb = new StringBuilder();
    sb.append("scribe_");
    sb.append(topic);
    sb.append("_");
    sb.append(partition);
    String r = sb.toString();
    System.out.println("client name: " + r); //xxx
    return r;
  }

  private long getLastOffset(String topic, int partition, long startTime) {
    TopicAndPartition tp = new TopicAndPartition(topic, partition);

    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(tp, new PartitionOffsetRequestInfo(startTime, 1));

    OffsetRequest req = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), getClientName());

    OffsetResponse resp = consumer.getOffsetsBefore(req);

    if (resp.hasError()) {
      System.out.println("error when fetching offset: " + resp.errorCode(topic, partition)); //xxx
      // In case you wonder what the error code really means.
      // System.out.println("OffsetOutOfRangeCode()" + ErrorMapping.OffsetOutOfRangeCode());
      // System.out.println("BrokerNotAvailableCode()" + ErrorMapping.BrokerNotAvailableCode());
      // System.out.println("InvalidFetchSizeCode()" + ErrorMapping.InvalidFetchSizeCode());
      // System.out.println("InvalidMessageCode()" + ErrorMapping.InvalidMessageCode());
      // System.out.println("LeaderNotAvailableCode()" + ErrorMapping.LeaderNotAvailableCode());
      // System.out.println("MessageSizeTooLargeCode()" + ErrorMapping.MessageSizeTooLargeCode());
      // System.out.println("NotLeaderForPartitionCode()" + ErrorMapping.NotLeaderForPartitionCode());
      // System.out.println("OffsetMetadataTooLargeCode()" + ErrorMapping.OffsetMetadataTooLargeCode());
      // System.out.println("ReplicaNotAvailableCode()" + ErrorMapping.ReplicaNotAvailableCode());
      // System.out.println("RequestTimedOutCode()" + ErrorMapping.RequestTimedOutCode());
      // System.out.println("StaleControllerEpochCode()" + ErrorMapping.StaleControllerEpochCode());
      // System.out.println("UnknownCode()" + ErrorMapping.UnknownCode());
      // System.out.println("UnknownTopicOrPartitionCode()" + ErrorMapping.UnknownTopicOrPartitionCode());

      //LOG.error("error when fetching offset: " + resp.errorcode(topic, partition));
      return 0;
    }

    return resp.offsets(topic, partition)[0];
  }

  public void run() throws IOException {
    long offset = getLastOffset(topic, partition, kafka.api.OffsetRequest.LatestTime());
    System.out.println(">> offset: " + offset); //xxx

    while (true) {
      System.out.println(">> offset: " + offset); //xxx
      FetchRequest req = new FetchRequestBuilder()
        .clientId(getClientName())
        .addFetch(topic, partition, offset, 100000)
        .build();

      FetchResponse resp = consumer.fetch(req);

      if (resp.hasError()) {
        //TODO: if we got an invalid offset, reset it by asking for the last element.
        // otherwise, find a new leader from the replica
        System.out.println("has error");  //xxx

        short code = resp.errorCode(topic, partition);
        System.out.println("Reason: " + code);
        if (code == ErrorMapping.OffsetOutOfRangeCode())  {
          // We asked for an invalid offset. For simple case ask for the last element to reset
          System.out.println("inside errormap");
          offset = getLastOffset(topic, partition, kafka.api.OffsetRequest.LatestTime());
          continue;
        }
      }

      long msgReadCnt = 0;

      StringRecordWriter writer = new StringRecordWriter("/tmp/scribe_data");
      for (MessageAndOffset messageAndOffset : resp.messageSet(topic, partition)) {
        long currentOffset = messageAndOffset.offset();
        if (currentOffset < offset) {
          System.out.println("Found an old offset: " + currentOffset + "Expecting: " + offset);
          continue;
        }
        offset = messageAndOffset.nextOffset();
        ByteBuffer payload = messageAndOffset.message().payload();

        byte[] bytes = new byte[payload.limit()];
        payload.get(bytes);

        System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes));
        // Write to HDFS /tmp partition
        writer.write(bytes);

        msgReadCnt++;
      }// for
      writer.close();

      if (msgReadCnt == 0) {
        try {
          System.out.println("Nothing to read, so go to sleep.");
          Thread.sleep(1000); //Didn't read anything, so go to sleep for awhile.
        } catch(InterruptedException e) {
        }
      }
    }
  }

  public static void main(String[] args) throws IOException {
    ScribeConsumer sc = new ScribeConsumer();
    JCommander jc = new JCommander(sc);
    jc.addConverterFactory(new CustomConvertFactory());
    jc.parse(args);

    sc.init();
    sc.run();
  }

}
