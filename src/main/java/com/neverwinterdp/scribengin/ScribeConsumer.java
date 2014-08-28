package com.neverwinterdp.scribengin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
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
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableList;

public class ScribeConsumer {
  // Random comments:
  // Unique define a partition. Client name + topic name + offset
  // java -cp scribengin-uber-0.0.1-SNAPSHOT.jar com.neverwinterdp.scribengin.ScribeConsumer --topic scribe  --leader 10.0.2.15:9092 --checkpoint_interval 100 --partition 0
  // checkout src/main/java/com/neverwinterdp/scribengin/ScribeConsumer.java
  // checkout org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
  // checkout EtlMultiOutputCommitter in Camus
  // /usr/lib/kafka/bin/kafka-console-producer.sh --topic scribe --broker-list 10.0.2.15:9092

  //TODO externalize these
  private static final String PRE_COMMIT_PATH_PREFIX = "/tmp";
  private static final String COMMIT_PATH_PREFIX = "/home/vagrant/hdfs";

  private static final Logger LOG = Logger.getLogger(ScribeConsumer.class.getName());

  private String currTmpDataPath;
  private String currDataPath;
  private AbstractScribeCommitLogFactory scribeCommitLogFactory;
  private AbstractFileSystemFactory fileSystemFactory;

  @Parameter(names = {"-" + Constants.OPT_KAFKA_TOPIC, "--" + Constants.OPT_KAFKA_TOPIC})
  private String topic;

  @Parameter(names = {"-" + Constants.OPT_LEADER, "--" + Constants.OPT_LEADER})
  private HostPort leaderHostPort; // "host:port"

  private List<Broker> brokerList;

  @Parameter(names = {"-" + Constants.OPT_PARTITION, "--" + Constants.OPT_PARTITION})
  private int partition;
  private long lastCommittedOffset;
  private long offset; // offset is on a per line basis. starts on the last valid offset

  @Parameter(names = {"-" + Constants.OPT_REPLICA, "--" + Constants.OPT_REPLICA},
      variableArity = true)
  private List<String> replicaBrokerList;

  @Parameter(
      names = {"-" + Constants.OPT_CHECK_POINT_TIMER, "--" + Constants.OPT_CHECK_POINT_TIMER},
      description = "Check point interval in milliseconds")
  private long commitCheckPointInterval; // ms

  private SimpleConsumer consumer;
  //private FileSystem fs;
  private Timer checkPointIntervalTimer;

  public ScribeConsumer() {
    checkPointIntervalTimer = new Timer();
  }

  public void setScribeCommitLogFactory(AbstractScribeCommitLogFactory factory) {
    scribeCommitLogFactory = factory;
  }

  public void setFileSystemFactory(AbstractFileSystemFactory factory) {
    fileSystemFactory = factory;
  }

  public void init() throws IOException {

    LOG.info("Leader " + leaderHostPort);
    consumer = new SimpleConsumer(
        leaderHostPort.getHost(),
        leaderHostPort.getPort(),
        10000, // timeout
        64 * 1024, // buffersize
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
    //TODO: move from tmp to the actual partition.
    LOG.info(">> committing");

    if (lastCommittedOffset != offset) {
      //Commit

      try {
        // First record the to-be taken action in the WAL.
        // Then, mv the tmp data file to it's location.
        long startOffset = lastCommittedOffset + 1;
        long endOffset = offset;
        LOG.info("\tstartOffset : " + String.valueOf(startOffset)); //xxx
        LOG.info("\tendOffset   : " + String.valueOf(endOffset)); //xxx
        LOG.info("\ttmpDataPath : " + currTmpDataPath); //xxx
        LOG.info("\tDataPath    : " + currDataPath); //xxx

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
    long ts = System.currentTimeMillis() / 1000L;
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
    LOG.info("getLatestOffsetFromKafka. topic: " + topic + " partition: " + partition
        + " startTime: " + startTime);
    TopicAndPartition tp = new TopicAndPartition(topic, partition);

    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(tp, new PartitionOffsetRequestInfo(startTime, 1));

    OffsetRequest req = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), getClientName());

    OffsetResponse resp = consumer.getOffsetsBefore(req);


    if (resp.hasError()) {// TODO get a way to break out of this.
      LOG.info("error when fetching offset: "
          + resp.errorCode(topic, partition));
      LOG.info("Is it because we are not talking to the leader? "
          + (resp.errorCode(topic, partition) == ErrorMapping
              .NotLeaderForPartitionCode()));
      LOG.info("Is it because we dont have such a topic? "
          + (resp.errorCode(topic, partition) == ErrorMapping
              .UnknownTopicOrPartitionCode()));
      if (resp.errorCode(topic, partition) == ErrorMapping
          .NotLeaderForPartitionCode()) {
        LOG.info("Going to look for a new leader.");
        consumer = createNewConsumer();
        resp = consumer.getOffsetsBefore(req);
      }

      // In case you wonder what the error code really means.
      //LOG.info("OffsetOutOfRangeCode()" + ErrorMapping.OffsetOutOfRangeCode());
      //LOG.info("BrokerNotAvailableCode()" + ErrorMapping.BrokerNotAvailableCode());
      //LOG.info("InvalidFetchSizeCode()" + ErrorMapping.InvalidFetchSizeCode());
      //LOG.info("InvalidMessageCode()" + ErrorMapping.InvalidMessageCode());
      //LOG.info("LeaderNotAvailableCode()" + ErrorMapping.LeaderNotAvailableCode());
      //LOG.info("MessageSizeTooLargeCode()" + ErrorMapping.MessageSizeTooLargeCode());
      //LOG.info("NotLeaderForPartitionCode()" + ErrorMapping.NotLeaderForPartitionCode());
      //LOG.info("OffsetMetadataTooLargeCode()" + ErrorMapping.OffsetMetadataTooLargeCode());
      //LOG.info("ReplicaNotAvailableCode()" + ErrorMapping.ReplicaNotAvailableCode());
      //LOG.info("RequestTimedOutCode()" + ErrorMapping.RequestTimedOutCode());
      //LOG.info("StaleControllerEpochCode()" + ErrorMapping.StaleControllerEpochCode());
      //LOG.info("UnknownCode()" + ErrorMapping.UnknownCode());
      //LOG.info("UnknownTopicOrPartitionCode()" + ErrorMapping.UnknownTopicOrPartitionCode());

      //LOG.error("error when fetching offset: " + resp.errorcode(topic, partition));
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
    LOG.info("createNewConsumer. ");
    Broker leader = null;
    ImmutableList<String> topicList = ImmutableList.of(topic);
    TopicMetadataRequest request = new TopicMetadataRequest(topicList);
    TopicMetadataResponse topicMetadataResponse = null;
    try {
      boolean success = false;
      while (!success) {
        topicMetadataResponse = consumer.send(request);
        success = true;
      }
    } catch (Exception e) {
      LOG.debug("Looping through " + brokerList.size() + " brokers");
      for (int i = 0; i < brokerList.size(); i++) {
        if (brokerList.get(i).host() == consumer.host()
            && brokerList.get(i).port() == consumer.port())
          brokerList.remove(i);
      }
      //connect to another broker
      Broker broker = brokerList.get(0);
      LOG.debug("Attempting to connect to  " + broker.port());
      consumer =
          new SimpleConsumer(broker.host(), broker.port(), 1000, 60 * 1024, getClientName());
      //  topicMetadataResponse = consumer.send(request);
    }

    for (TopicMetadata topicMetadata : topicMetadataResponse
        .topicsMetadata()) {
      LOG.info("topic metadata " + topicMetadata.partitionsMetadata().size());
      for (PartitionMetadata partitionMetadata : topicMetadata
          .partitionsMetadata()) {
        LOG.info("partition " + partitionMetadata.partitionId());
        if (partitionMetadata.partitionId() == partition) {
          //save these members somewhere
          LOG.info("Members " + partitionMetadata.replicas());
          brokerList = partitionMetadata.replicas();
          LOG.info("We've found a leader. "
              + partitionMetadata.leader().getConnectionString());
          leader = partitionMetadata.leader();
        }
      }
    }
    LOG.info("Leader " + leader);
    SimpleConsumer updatedConsumer = new SimpleConsumer(leader.host(),
        leader.port(), 10000, // timeout
        64 * 1024, // buffer size
        getClientName());
    LOG.info("Succesfully created a new kafka consumer. "+consumer.host() +":" +consumer.port());
    return updatedConsumer;
  }

  private void DeleteUncommittedData() throws IOException
  {
    FileSystem fs = fileSystemFactory.build();

    // Corrupted log file
    // Clean up. Delete the tmp data file if present.
    FileStatus[] fileStatusArry = fs.globStatus(new Path(getTmpDataPathPattern()));
    for (int i = 0; i < fileStatusArry.length; i++) {
      FileStatus fileStatus = fileStatusArry[i];
      fs.delete(fileStatus.getPath());
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
    } catch (NullPointerException e) {
      LOG.info("No log.");
      //    e.printStackTrace();
    }

    if (entry != null) {
      r = entry.getEndOffset();
    }

    return r;
  }

  private long getLatestOffset(String topic, int partition, long startTime) {
    long offsetFromCommitLog = getLatestOffsetFromCommitLog();
    LOG.info(" getLatestOffsetFromCommitLog >>>> " + offsetFromCommitLog); //xxx
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
    LOG.info(">> lastCommittedOffset: " + lastCommittedOffset); //xxx

    while (true) {
      LOG.info(">> offset: " + offset); //xxx
      FetchRequest req = new FetchRequestBuilder()
          .clientId(getClientName())
          .addFetch(topic, partition, offset, 100000)
          .build();

      FetchResponse resp = consumer.fetch(req);

      if (resp.hasError()) {
        //TODO: if we got an invalid offset, reset it by asking for the last element.
        // otherwise, find a new leader from the replica
        LOG.info("has error"); //xxx

        short code = resp.errorCode(topic, partition);
        LOG.info("Reason: " + code);
        if (code == ErrorMapping.OffsetOutOfRangeCode()) {
          // We asked for an invalid offset. For simple case ask for the last element to reset
          LOG.info("inside errormap");
          offset = getLatestOffsetFromKafka(topic, partition, kafka.api.OffsetRequest.LatestTime());
          continue;
        }
      }

      long msgReadCnt = 0;

      StringRecordWriter writer = new StringRecordWriter(currTmpDataPath);
      synchronized (this) {
        for (MessageAndOffset messageAndOffset : resp.messageSet(topic, partition)) {
          long currentOffset = messageAndOffset.offset();
          if (currentOffset < offset) {
            LOG.info("Found an old offset: " + currentOffset + "Expecting: " + offset);
            continue;
          }
          offset = messageAndOffset.nextOffset();
          ByteBuffer payload = messageAndOffset.message().payload();

          byte[] bytes = new byte[payload.limit()];
          payload.get(bytes);

          //    LOG.info(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes));
          // Write to HDFS /tmp partition
          writer.write(bytes);

          msgReadCnt++;
        }// for
      }
      writer.close();

      if (msgReadCnt == 0) {
        try {
          Thread.sleep(1000); //Didn't read anything, so go to sleep for awhile.
        } catch (InterruptedException e) {
        }
      }
    } // while
  }

  public SimpleConsumer getConsumer() {
    return consumer;
  }

  public void setConsumer(SimpleConsumer consumer) {
    this.consumer = consumer;
  }

  public HostPort getLeaderHostPort() {
    return leaderHostPort;
  }

  public void setLeaderHostPort(HostPort leaderHostPort) {
    this.leaderHostPort = leaderHostPort;
  }

  public static void main(String[] args) throws IOException {

    /* String[] args1 = {"--topic", "scribengin", "--leader", "192.168.33.33:9094",
         "--checkpoint_interval", "1000", "--partition", "0"};*/
    LOG.info("args " + Arrays.toString(args));
    ScribeConsumer sc = new ScribeConsumer();
    JCommander jc = new JCommander(sc);
    jc.addConverterFactory(new CustomConvertFactory());
    jc.parse(args);

    sc.setScribeCommitLogFactory(ScribeCommitLogFactory.instance(sc.getCommitLogAbsPath()));
    sc.setFileSystemFactory(FileSystemFactory.instance());
    sc.init();
    sc.run();
  }

}
