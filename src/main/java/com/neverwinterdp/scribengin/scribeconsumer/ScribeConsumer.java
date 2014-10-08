package com.neverwinterdp.scribengin.scribeconsumer;

import java.io.IOException;
import java.net.URI;
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
import com.beust.jcommander.ParameterException;
import com.neverwinterdp.scribengin.commitlog.AbstractScribeCommitLogFactory;
import com.neverwinterdp.scribengin.commitlog.ScribeCommitLog;
import com.neverwinterdp.scribengin.commitlog.ScribeCommitLogFactory;
import com.neverwinterdp.scribengin.commitlog.ScribeLogEntry;
import com.neverwinterdp.scribengin.filesystem.AbstractFileSystemFactory;
import com.neverwinterdp.scribengin.filesystem.FileSystemFactory;
import com.neverwinterdp.scribengin.filesystem.HDFSFileSystemFactory;
import com.neverwinterdp.scribengin.hostport.CustomConvertFactory;
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.neverwinterdp.scribengin.partitioner.AbstractPartitioner;
import com.neverwinterdp.scribengin.partitioner.DatePartitioner;
import com.neverwinterdp.scribengin.partitioner.DumbPartitioner;
import com.neverwinterdp.scribengin.utilities.LostLeadershipException;
import com.neverwinterdp.scribengin.utilities.StringRecordWriter;

public class ScribeConsumer {
  // Random comments:
  // Unique define a partition. Client name + topic name + offset
  // java -cp scribengin-1.0-SNAPSHOT.jar com.neverwinterdp.scribengin.ScribeConsumer --topic scribe --broker_list HOST1:PORT,HOST2:PORT2 --checkpoint_interval 100 --partition 0
  // checkout src/main/java/com/neverwinterdp/scribengin/ScribeConsumer.java
  // checkout org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
  // checkout EtlMultiOutputCommitter in Camus
  // /usr/lib/kafka/bin/kafka-console-producer.sh --topic scribe --broker-list 10.0.2.15:9092

  //Variables set by constructor method/command line parameters
  private String PRE_COMMIT_PATH_PREFIX;
  private String COMMIT_PATH_PREFIX;
  private String topic;
  private int partition;
  private List<HostPort> brokerList; // list of (host:port)s
  private long commitCheckPointInterval; // ms
  private String hdfsPath = null;
  //private String libhadoopPath = "/usr/lib/hadoop/lib/native/libhadoop.so";

  //Set by cleanStart() method
  private boolean cleanStart = false;

  //Instantiated at time of object instantiation
  private Timer checkPointIntervalTimer;
  private Timer partitionerUpdateTimer;
  private List<HostPort> replicaBrokers; // list of (host:port)s

  //Private class variables
  private AbstractPartitioner partitioner = null;
  private String currTmpDataPath;
  private String currDataPath;
  private AbstractScribeCommitLogFactory scribeCommitLogFactory;
  private AbstractFileSystemFactory fileSystemFactory;
  private long lastCommittedOffset;
  private long offset; // offset is on a per line basis. starts on the last valid offset
  private Thread serverThread;
  private SimpleConsumer consumer;
  private static final Logger LOG = Logger.getLogger(ScribeConsumer.class.getName());



  public ScribeConsumer() {
    checkPointIntervalTimer = new Timer();
    partitionerUpdateTimer = new Timer();
    replicaBrokers = new ArrayList<HostPort>();
  }

  public ScribeConsumer(ScribeConsumerConfig c) {
    this();
    this.PRE_COMMIT_PATH_PREFIX = c.PRE_COMMIT_PATH_PREFIX;
    this.COMMIT_PATH_PREFIX = c.COMMIT_PATH_PREFIX;
    this.topic = c.topic;
    this.partition = c.partition;
    this.brokerList = c.brokerList;
    this.commitCheckPointInterval = c.commitCheckPointInterval;
    this.cleanStart = c.cleanStart;
    this.hdfsPath = c.hdfsPath;
    if (c.date_partitioner != null) {
      this.setPartitioner(new DatePartitioner(c.date_partitioner));
    }
    //this.libhadoopPath = c.libHadoopPath;
  }

  public ScribeConsumer(String preCommitPathPrefix, String commitPathPrefix, String topic,
      int partition, List<HostPort> brokerList, long commitCheckPointInterval) {
    this();
    this.PRE_COMMIT_PATH_PREFIX = preCommitPathPrefix;
    this.COMMIT_PATH_PREFIX = commitPathPrefix;
    this.topic = topic;
    this.partition = partition;
    this.brokerList = brokerList;
    this.commitCheckPointInterval = commitCheckPointInterval;
  }

  public ScribeConsumer(String preCommitPathPrefix, String commitPathPrefix, String topic,
      int partition, List<HostPort> brokerList, long commitCheckPointInterval, boolean cleanStart,
      String hdfsPath) {
    this(preCommitPathPrefix, commitPathPrefix, topic, partition, brokerList,
        commitCheckPointInterval);
    this.cleanStart = cleanStart;
    this.hdfsPath = hdfsPath;
  }

  /**
   * Call this method before calling init()
   * @param p
   */
  public void setPartitioner(AbstractPartitioner p) {
    this.partitioner = p;
  }

  public void setScribeCommitLogFactory(AbstractScribeCommitLogFactory factory) {
    scribeCommitLogFactory = factory;
  }

  public void setFileSystemFactory(AbstractFileSystemFactory factory) {
    fileSystemFactory = factory;
  }

  public void init() throws IOException {
    if (partitioner == null) {
      partitioner = new DumbPartitioner();
    }

    if (this.hdfsPath == null) {
      setScribeCommitLogFactory(ScribeCommitLogFactory.instance(getCommitLogAbsPath()));
      setFileSystemFactory(FileSystemFactory.instance());
    }
    else {
      setScribeCommitLogFactory(ScribeCommitLogFactory.instance(this.hdfsPath
          + getCommitLogAbsPath()));
      setFileSystemFactory(HDFSFileSystemFactory.instance());
    }
  }

  public boolean connectToTopic() {
    boolean r = true;
    PartitionMetadata metadata = findLeader(brokerList, topic, partition);
    if (metadata == null) {
      r = false;
      LOG.error("Can't find meta data for Topic: " + topic + " partition: " + partition
          + ". In fact, meta is null.");
    }
    else if (metadata.leader() == null) {
      r = false;
      LOG.error("Can't find meta data for Topic: " + topic + " partition: " + partition);
    }

    if (r) {
      storeReplicaBrokers(metadata);

      consumer = new SimpleConsumer(
          metadata.leader().host(),
          metadata.leader().port(),
          10000, // timeout
          64 * 1024, // buffersize
          getClientName());

      scheduleCommitTimer();
    }
    return r;
  }

  public void start() {
    serverThread = new Thread() {
      public void run() {
        try {
          runServerLoop();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    serverThread.start();
  }

  public Thread.State getServerState() {
    return serverThread.getState();
  }

  public void stop() {
    try {
      checkPointIntervalTimer.cancel();
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      serverThread.interrupt();
    } catch (Exception e) {
      e.printStackTrace();
    }
    commit();
  }

  private void scheduleCommitTimer() {
    checkPointIntervalTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        commit();
      }
    }, commitCheckPointInterval);
  }

  private void schedulePartitionUpdateTime() {
    if (this.partitioner.getRefresh() == null) {
      return;
    }
    this.partitionerUpdateTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        commit();
      }
    }, partitioner.getRefresh());
  }

  private void commitData(String src, String dest) {
    FileSystem fs = null;
    Path destPath = new Path(dest);

    try {
      fs = fileSystemFactory.build(URI.create(src));
      if (!fs.exists(destPath.getParent())) {
        fs.mkdirs(destPath.getParent());
      }
      fs.rename(new Path(src), destPath);
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
    LOG.info(">> committing: " + this.topic);

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
    try{
      scheduleCommitTimer();
    } catch (IllegalStateException e){
      LOG.error("Could not reschedule commit timer.  Ignore if this is during ScribeConsumer shutdown.");
    }
  }

  private String getClientName() {
    return "scribe_" + topic + "_" + partition;
  }

  private String getCommitLogAbsPath() {
    return PRE_COMMIT_PATH_PREFIX + "/" + getClientName() + ".log";
  }

  private void generateTmpAndDestDataPaths() {
    long ts = System.currentTimeMillis() / 1000L;

    this.currTmpDataPath = PRE_COMMIT_PATH_PREFIX + "/scribe.data." + ts;
    this.currDataPath =
        COMMIT_PATH_PREFIX + "/" + this.partitioner.getPartition() + "/scribe.data." + ts;

    if (this.hdfsPath != null) {
      this.currTmpDataPath = this.hdfsPath + this.currTmpDataPath;
      this.currDataPath = this.hdfsPath + this.currDataPath;
    }

    schedulePartitionUpdateTime();
  }

  private String getTmpDataPathPattern() {
    return PRE_COMMIT_PATH_PREFIX + "/scribe.data.*";
  }

  private long getLatestOffsetFromKafka(String topic, int partition, long startTime) {
    TopicAndPartition tp = new TopicAndPartition(topic, partition);

    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(tp, new PartitionOffsetRequestInfo(startTime, 1));

    OffsetRequest req = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), getClientName());

    OffsetResponse resp = consumer.getOffsetsBefore(req);

    if (resp.hasError()) {
      LOG.error("error when fetching offset: " + resp.errorCode(topic, partition)); //xxx
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

  private long getEarliestOffsetFromKafka(String topic, int partition, long startTime) {
    LOG.info("getEarliestOffsetFromKafka.");
    TopicAndPartition tp = new TopicAndPartition(topic, partition);

    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
        new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(tp, new PartitionOffsetRequestInfo(startTime, 1));

    OffsetRequest req = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), getClientName());

    OffsetResponse resp = consumer.getOffsetsBefore(req);

    if (resp.hasError()) {
      LOG.error("error when fetching offset: " + resp.errorCode(topic, partition)); //xxx
      return 0;
    }
    LOG.info("Earliest offset " + resp.offsets(topic, partition)[0]);
    return resp.offsets(topic, partition)[0];
  }

  private void DeleteUncommittedData() throws IOException
  {
    FileSystem fs;
    if (this.hdfsPath != null) {
      fs = fileSystemFactory.build(URI.create(this.hdfsPath));
    }
    else {
      fs = fileSystemFactory.build();
    }
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

      if (entry != null && entry.isCheckSumValid()) {
        String tmpDataFilePath = entry.getSrcPath();
        FileSystem fs = fileSystemFactory.build(URI.create(tmpDataFilePath));

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

  private PartitionMetadata findLeader(List<HostPort> seedBrokers, String topic, int partition) {
    PartitionMetadata returnMetaData = null;

    for (HostPort broker : seedBrokers) {
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
              return returnMetaData;
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Error communicating with Broker " + seed + ":" + port
            + " while trying to find leader for " + topic
            + ", " + partition + " | Reason: " + e);
      } finally {
        if (consumer != null)
          consumer.close();
      }
    }

    return returnMetaData;
  }

  private HostPort findNewLeader(String oldHost, int oldPort) throws LostLeadershipException {
    for (int i = 0; i < 3; i++) {
      boolean goToSleep = false;
      PartitionMetadata metadata = findLeader(replicaBrokers, topic, partition);
      if (metadata == null) {
        goToSleep = true;
      } else if (metadata.leader() == null) {
        goToSleep = true;
      } else if (oldHost.equalsIgnoreCase(metadata.leader().host()) &&
          oldPort == metadata.leader().port()) {
        // first time through if the leader hasn't changed give ZooKeeper a second to recover
        // second time, assume the broker did recover before failover, or it was a non-Broker issue
        goToSleep = true;
      } else {
        return new HostPort(metadata.leader().host(), metadata.leader().port());
      }
      if (goToSleep) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }
    // Can't recover from a leadership disappearance.
    throw new LostLeadershipException();
  }

  private void storeReplicaBrokers(PartitionMetadata metadata) {
    replicaBrokers.clear();
    for (Broker replica : metadata.replicas()) {
      replicaBrokers.add(new HostPort(replica.host(), replica.port()));
    }
  }

  public void runServerLoop() throws IOException, LostLeadershipException, InterruptedException {
    LOG.info("Are we clean starting? " + cleanStart);
    int retry = 0;
    int retryLimit = 10;
    while (!connectToTopic()) {
      LOG.error("Could not connect to topic...");
      Thread.sleep(1000);
      retry++;
      if (retry > retryLimit) {
        return;
      }
    }

    generateTmpAndDestDataPaths();
    // lastCommittedOffset = getLatestOffset(topic, partition, kafka.api.OffsetRequest.LatestTime());
    //TODO clear commit log
    if (cleanStart) {
      offset = getEarliestOffsetFromKafka(topic, partition, kafka.api.OffsetRequest.EarliestTime());
      lastCommittedOffset = offset;
      DeleteUncommittedData();
      clearCommitLog();
    }
    else {
      lastCommittedOffset = getLatestOffset(topic, partition, kafka.api.OffsetRequest.LatestTime());
      long earliestOffset =
          getEarliestOffsetFromKafka(topic, partition, kafka.api.OffsetRequest.EarliestTime());
      if (earliestOffset == lastCommittedOffset) {
        throw new IllegalStateException(
            "Scribe consumer is consuming from the first offset yet --clean_start was not defined.");
      }
      offset = lastCommittedOffset;
    }
    LOG.info(">> lastCommittedOffset: " + lastCommittedOffset); //xxx

    while (true) {
      LOG.info(">> offset: " + offset); //xxx
      FetchRequest req = new FetchRequestBuilder()
          .clientId(getClientName())
          .addFetch(topic, partition, offset, 100000)
          .build();

      FetchResponse resp = consumer.fetch(req);

      if (resp.hasError()) {
        //If we got an invalid offset, reset it by asking for the last element.
        //For all other errors, assume the worst and find ourselves a new leader from the replica.
        short code = resp.errorCode(topic, partition);
        LOG.info("Encounter error when fetching from consumer. Error Code: " + code);
        if (code == ErrorMapping.OffsetOutOfRangeCode()) {
          // We asked for an invalid offset. For simple case ask for the last element to reset
          LOG.info("inside errormap");
          offset = getLatestOffsetFromKafka(topic, partition, kafka.api.OffsetRequest.LatestTime());
          continue;
        } else {
          String oldHost = consumer.host();
          int oldPort = consumer.port();
          consumer.close();
          consumer = null;
          HostPort newHostPort = findNewLeader(oldHost, oldPort);
          consumer = new SimpleConsumer(
              newHostPort.getHost(),
              newHostPort.getPort(),
              10000, // timeout
              64 * 1024, // buffersize
              getClientName());
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

          LOG.info(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes));
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

  private void clearCommitLog() throws IOException {
    ScribeCommitLog log = scribeCommitLogFactory.build();
    log.clear();
  }

  public void cleanStart(boolean b) {
    cleanStart = b;
  }

  public static void main(String[] args) throws IOException {
    ScribeConsumerCommandLineArgs p = new ScribeConsumerCommandLineArgs();

    JCommander jc = new JCommander(p);
    jc.addConverterFactory(new CustomConvertFactory());
    try {
      jc.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jc.usage();
      System.exit(-1);
    }

    ScribeConsumer sc =
        new ScribeConsumer(p.preCommitPrefix, p.commitPrefix, p.topic, p.partition, p.brokerList,
            p.commitCheckPointInterval, p.cleanstart, p.hdfsPath);
    if (p.date_partitioner != null) {
      sc.setPartitioner(new DatePartitioner(p.date_partitioner));
    }

    sc.init();


    try {
      sc.runServerLoop();
    } catch (LostLeadershipException e) {
      LOG.fatal("Leader went away. Couldn't find a new leader!");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

}
