package com.neverwinterdp.scribengin;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ScribeConsumer {
  // Random comments:
  // Unique define a partition. Client name + topic name + offset
  // java -cp scribengin-uber-0.0.1-SNAPSHOT.jar com.neverwinterdp.scribengin.ScribeConsumer --topic scribe  --leader 10.0.2.15:9092 --checkpoint_interval 100 --partition 0
  // checkout src/main/java/com/neverwinterdp/scribengin/ScribeConsumer.java
  // checkout org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
  // checkout EtlMultiOutputCommitter in Camus
  // /usr/lib/kafka/bin/kafka-console-producer.sh --topic scribe --broker-list 10.0.2.15:9092

  private static final String PRE_COMMIT_PATH_PREFIX = "/tmp";
  private static final String COMMIT_PATH_PREFIX = "/user/kxae";

  private static final Logger LOG = Logger.getLogger(ScribeConsumer.class.getName());

  private String currTmpDataPath;
  private String currDataPath;

  @Parameter(names = {"-"+Constants.OPT_KAFKA_TOPIC, "--"+Constants.OPT_KAFKA_TOPIC})
  private String topic;

  @Parameter(names = {"-"+Constants.OPT_LEADER, "--"+Constants.OPT_LEADER})
  private HostPort leaderHostPort; // "host:port"

  @Parameter(names = {"-"+Constants.OPT_PARTITION, "--"+Constants.OPT_PARTITION})
  private int partition;
  private long lastCommittedOffset;
  private long offset; // offset is on a per line basis. starts on the last valid offset

  @Parameter(names = {"-"+Constants.OPT_REPLICA, "--"+Constants.OPT_REPLICA}, variableArity = true)
  private List<String> replicaBrokerList;

  @Parameter(names = {"-"+Constants.OPT_CHECK_POINT_TIMER, "--"+Constants.OPT_CHECK_POINT_TIMER}, description="Check point interval in milliseconds")
  private long commitCheckPointInterval; // ms

  private SimpleConsumer consumer;
  //private FileSystem fs;
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

  private FileSystem getFS() throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    FileSystem fs = FileSystem.get(conf);
    //fs = FileSystem.get(URI.create(COMMIT_PATH_PREFIX), conf);
    return fs;
  }

  private void commitData(String src, String dest) {
    FileSystem fs = null;
    try {
      fs = getFS();
      fs.rename(new Path(src), new Path(dest));
    } catch (IOException e) {
      //TODO : LOG
      e.printStackTrace();
    } finally {
      if (fs != null) {
        try {
          fs.close();
        } catch (IOException e) {
          //TODO : LOG
          e.printStackTrace();
        }
      }
    }
  }

  private synchronized void commit() {
    //TODO: move from tmp to the actual partition.
    System.out.println(">> committing");

    if (lastCommittedOffset != offset) {
      //Commit

      try {
        // First record the to-be taken action in the WAL.
        // Then, mv the tmp data file to it's location.
        long startOffset = lastCommittedOffset + 1;
        long endOffset = offset;
        System.out.println("\tstartOffset : " + String.valueOf(startOffset)); //xxx
        System.out.println("\tendOffset   : " + String.valueOf(endOffset)); //xxx
        System.out.println("\ttmpDataPath : " + currTmpDataPath); //xxx
        System.out.println("\tDataPath    : " + currDataPath); //xxx

        ScribeCommitLog log = new ScribeCommitLog(getCommitLogAbsPath());
        log.record(startOffset, endOffset, currTmpDataPath, currDataPath);
        commitData(currTmpDataPath, currDataPath);

        lastCommittedOffset = offset;
        generateTmpAndDestDataPaths();
      } catch (IOException e) {
        // TODO : LOG this error
        e.printStackTrace();
      } catch (NoSuchAlgorithmException e) {
        // TODO : LOG this error
        e.printStackTrace();
      }
    }

    scheduleCommitTimer();
  }

  private String getClientName() {
    StringBuilder sb = new StringBuilder();
    sb.append("scribe_");
    sb.append(topic);
    sb.append("_");
    sb.append(partition);
    String r = sb.toString();
    return r;
  }

  private String getCommitLogAbsPath() {
    return PRE_COMMIT_PATH_PREFIX + "/" + getClientName() + ".log";
  }

  private void generateTmpAndDestDataPaths() {
    StringBuilder sb = new StringBuilder();
    long ts = System.currentTimeMillis()/1000L;
    sb.append(PRE_COMMIT_PATH_PREFIX)
      .append("/scribe.data")
      .append(".")
      .append(ts);
    this.currTmpDataPath = sb.toString();

    sb = new StringBuilder();
    sb.append(COMMIT_PATH_PREFIX)
      .append("/scribe.data")
      .append(".")
      .append(ts);
    this.currDataPath = sb.toString();
  }

  private String getTmpDataPathPattern() {
    StringBuilder sb = new StringBuilder();
    sb.append(PRE_COMMIT_PATH_PREFIX)
      .append("/scribe.data")
      .append(".")
      .append("*");
    return sb.toString();
  }

  private long getLatestOffsetFromKafka(String topic, int partition, long startTime) {
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

  private long getLatestOffsetFromCommitLog() {
    // Here's where we do recovery.
    long r = -1;

    try {
      ScribeCommitLog log = new ScribeCommitLog(getCommitLogAbsPath());
      log.readLastTwoEntries();
      ScribeLogEntry entry = log.getLatestEntry();

      FileSystem fs = getFS();
      if (entry.isCheckSumValid()) {
        String tmpDataFilePath = entry.getSrcPath();


      } else {
        // Corrupted log file
        // Clean up. Delete the tmp data file if present.
        FileStatus[] fileStatusArry = fs.globStatus(new Path(getTmpDataPathPattern()));
        for(int i = 0; i < fileStatusArry.length; i++) {
          FileStatus fileStatus = fileStatusArry[i];
          fs.delete( fileStatus.getPath() );
        }

        // Read the next log entry.
        entry = log.getLatestEntry();
        r = entry.getEndOffset();
      }
    } catch (IOException e) {
      //TODO: log.warn
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      //TODO: log.warn
      e.printStackTrace();
    }
    return r;
  }

  private long getLatestOffset(String topic, int partition, long startTime) {
    long offsetFromCommitLog = getLatestOffsetFromCommitLog();
    System.out.println(" getLatestOffsetFromCommitLog >>>> " + offsetFromCommitLog); //xxx
    long offsetFromKafka = getLatestOffsetFromKafka(topic, partition, startTime);
    long r;
    if (offsetFromCommitLog == -1) {
      r = offsetFromKafka;
    } else if (offsetFromCommitLog < offsetFromKafka) {
      r = offsetFromCommitLog;
    } else if (offsetFromCommitLog == offsetFromKafka) {
      r = offsetFromKafka;
    } else { // offsetFromCommitLog > offsetFromKafka
      // TODO: log.warn. Someone is screwing with kafka's offset
      r = offsetFromKafka;
    }
    return r;
  }

  public void run() throws IOException {
    generateTmpAndDestDataPaths();
    lastCommittedOffset = getLatestOffset(topic, partition, kafka.api.OffsetRequest.LatestTime());
    offset = lastCommittedOffset;
    System.out.println(">> lastCommittedOffset: " + lastCommittedOffset); //xxx

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
          offset = getLatestOffsetFromKafka(topic, partition, kafka.api.OffsetRequest.LatestTime());
          continue;
        }
      }

      long msgReadCnt = 0;

      StringRecordWriter writer = new StringRecordWriter(currTmpDataPath);
      synchronized(this) {
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
      }
      writer.close();

      if (msgReadCnt == 0) {
        try {
          Thread.sleep(1000); //Didn't read anything, so go to sleep for awhile.
        } catch(InterruptedException e) {
        }
      }
    } // while
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
