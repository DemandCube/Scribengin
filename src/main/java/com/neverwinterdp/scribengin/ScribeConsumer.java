package com.neverwinterdp.scribengin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

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
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class ScribeConsumer {
  // Random comments:
  // Unique define a partition. Client name + topic name + offset
  // java -cp scribengin-uber-0.0.1-SNAPSHOT.jar com.neverwinterdp.scribengin.ScribeConsumer  --broker-lst  HOST1:PORT1,HOST2:PORT2 --checkpoint_interval 100 --partition 0 --topic scribe
  // checkout src/main/java/com/neverwinterdp/scribengin/ScribeConsumer.java
  // checkout org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
  // checkout EtlMultiOutputCommitter in Camus
  // /usr/lib/kafka/bin/kafka-console-producer.sh --topic scribe --broker-list 10.0.2.15:9092

  private static final String PRE_COMMIT_PATH_PREFIX = "/tmp";
  private static final String COMMIT_PATH_PREFIX = "/user/kxae";

  private static final Logger LOG = Logger.getLogger(ScribeConsumer.class.getName());

  private String currTmpDataPath;
  private String currDataPath;
  private AbstractScribeCommitLogFactory scribeCommitLogFactory;
  private AbstractFileSystemFactory fileSystemFactory;
  private List<HostPort> replicaBrokers; // list of (host:port)s

  @Parameter(names = {"-"+Constants.OPT_KAFKA_TOPIC, "--"+Constants.OPT_KAFKA_TOPIC})
  private String topic;

  @Parameter(names = {"-"+Constants.OPT_PARTITION, "--"+Constants.OPT_PARTITION})
  private int partition;
  private long lastCommittedOffset;
  private long offset; // offset is on a per line basis. starts on the last valid offset

  @Parameter(names = {"-"+Constants.OPT_BROKER_LIST, "--"+Constants.OPT_BROKER_LIST}, variableArity = true)
  private List<HostPort> brokerList; // list of (host:port)s

  @Parameter(names = {"-"+Constants.OPT_CHECK_POINT_TIMER, "--"+Constants.OPT_CHECK_POINT_TIMER}, description="Check point interval in milliseconds")
  private long commitCheckPointInterval; // ms

  private SimpleConsumer consumer;
  //private FileSystem fs;
  private Timer checkPointIntervalTimer;

  public ScribeConsumer() {
    checkPointIntervalTimer = new Timer();
    replicaBrokers = new ArrayList<HostPort>();
  }

  public void setScribeCommitLogFactory(AbstractScribeCommitLogFactory factory) {
    scribeCommitLogFactory = factory;
  }

  public void setFileSystemFactory(AbstractFileSystemFactory factory) {
    fileSystemFactory = factory;
  }

  public boolean init() throws IOException {
    boolean r = true;
    PartitionMetadata metadata = findLeader(brokerList, topic, partition);
    if (metadata == null) {
      r = false;
      LOG.error("Can't find meta data for Topic: " + topic + " partition: " + partition + ". In fact, meta is null.");
    }
    if (metadata.leader() == null) {
      r = false;
      LOG.error("Can't find meta data for Topic: " + topic + " partition: " + partition);
    }

    if (r) {
      storeReplicaBrokers(metadata);

      consumer = new SimpleConsumer(
          metadata.leader().host(),
          metadata.leader().port(),
          10000,   // timeout
          64*1024, // buffersize
          getClientName());

      scheduleCommitTimer();
    }
    return r;
  }

  private void scheduleCommitTimer() {
    checkPointIntervalTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        commit();
      }
    }, commitCheckPointInterval);
  }

  private void commitData(String src, String dest) {
    FileSystem fs = null;
    try {
      fs = fileSystemFactory.build();
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

        ScribeCommitLog log = scribeCommitLogFactory.build();
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

  private void DeleteUncommittedData() throws IOException
  {
    FileSystem fs = fileSystemFactory.build();

    // Corrupted log file
    // Clean up. Delete the tmp data file if present.
    FileStatus[] fileStatusArry = fs.globStatus(new Path(getTmpDataPathPattern()));
    for(int i = 0; i < fileStatusArry.length; i++) {
      FileStatus fileStatus = fileStatusArry[i];
      fs.delete( fileStatus.getPath() );
    }
    fs.close();
  }

  private ScribeLogEntry getLatestValidEntry(ScribeCommitLog log) {
    ScribeLogEntry entry = null;
    try {
      do {
        entry = log.getLatestEntry();
      } while (entry != null && !entry.isCheckSumValid());
    } catch (NoSuchAlgorithmException ex) {
      //TODO log
    }
    return entry;
  }

  private long getLatestOffsetFromCommitLog() {
    // Here's where we do recovery.
    long r = -1;

    ScribeLogEntry entry = null;
    try {
      ScribeCommitLog log = scribeCommitLogFactory.build();

      log.read();
      entry = log.getLatestEntry();

      if (entry.isCheckSumValid()) {
        FileSystem fs = fileSystemFactory.build();

        String tmpDataFilePath = entry.getSrcPath();

        if (fs.exists(new Path(tmpDataFilePath))) {
          // mv to the dest
          commitData(tmpDataFilePath, entry.getDestPath());
        } else {
          // Data has been committed
          // Or, it never got around to write to the log.
          // Delete tmp data file just in case.
          DeleteUncommittedData();
        }
      } else {
        DeleteUncommittedData();
        entry = getLatestValidEntry(log);
      }
    } catch (IOException e) {
      //TODO: log.warn
      e.printStackTrace();
    } catch (NoSuchAlgorithmException e) {
      //TODO: log.warn
      e.printStackTrace();
    }

    if (entry != null) {
      r = entry.getEndOffset();
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

  private PartitionMetadata findLeader(List<HostPort> seedBrokers, String topic, int partition) {
    PartitionMetadata returnMetaData = null;

    for (HostPort broker: seedBrokers) {
      SimpleConsumer consumer = null;
      String seed = broker.getHost();
      int port = broker.getPort();

      try {
        consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
        List<String> topics = Collections.singletonList(topic);
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

        List<TopicMetadata> metaData = resp.topicsMetadata();
        for (TopicMetadata item : metaData) {
          for (PartitionMetadata part : item.partitionsMetadata()) {
            if (part.partitionId() == partition) {
              returnMetaData = part;
              return  returnMetaData;
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Error communicating with Broker " + seed + ":" + port + " while trying to find leader for " + topic
            + ", " + partition + " | Reason: " + e);
      } finally {
        if (consumer != null) consumer.close();
      }
    }

    return returnMetaData;
  }

  private void storeReplicaBrokers(PartitionMetadata metadata) {
    replicaBrokers.clear();
    for (Broker replica: metadata.replicas()) {
      replicaBrokers.add(new HostPort(replica.host(), replica.port()));
    }
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

    sc.setScribeCommitLogFactory(ScribeCommitLogFactory.instance(sc.getCommitLogAbsPath()));
    sc.setFileSystemFactory(FileSystemFactory.instance());
    boolean proceed = sc.init();
    if (proceed)
      sc.run();
  }

}
