package com.neverwinterdp.scribengin;
import java.io.IOException;
import java.net.URI;
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
  private static final String COMMIT_PATH_PREFIX = "/committed";

  private static final Logger log = Logger.getLogger(ScribeConsumer.class.getName());

  private String currTmpDataPath;
  private String currDataPath;
  private AbstractScribeCommitLogFactory scribeCommitLogFactory;
  private AbstractFileSystemFactory fileSystemFactory;

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

  @Parameter(names = {"-"+Constants.OPT_HDFS_PATH, "--"+Constants.OPT_HDFS_PATH}, description="Location of HDFS")
  private String hdfsPath="";

  
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

  private void commitData(String src, String dest) {
    FileSystem fs = null;
    Path destPath = new Path(dest);
    
    try {
      fs = fileSystemFactory.build(URI.create(src));
      if(!fs.exists(destPath.getParent())){
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

  public String getHdfsPath(){
    return this.hdfsPath;
  }
  private synchronized void commit() {
    //TODO: move from tmp to the actual partition.
    log.info(">> committing");
    if (lastCommittedOffset != offset) {
      //Commit
      try {
        // First record the to-be taken action in the WAL.
        // Then, mv the tmp data file to it's location.
        long startOffset = lastCommittedOffset + 1;
        long endOffset = offset;
        log.info("\tstartOffset : " + String.valueOf(startOffset)); 
        log.info("\tendOffset   : " + String.valueOf(endOffset)); 
        log.info("\ttmpDataPath : " + currTmpDataPath); 
        log.info("\tDataPath    : " + currDataPath); 

        ScribeCommitLog sclog = scribeCommitLogFactory.build();
        sclog.record(startOffset, endOffset, currTmpDataPath, currDataPath);
        log.info("ATOMIC MOVE OF DATA");
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
    return "scribe_"+topic+"_"+partition;
    /*
    StringBuilder sb = new StringBuilder();
    sb.append("scribe_");
    sb.append(topic);
    sb.append("_");
    sb.append(partition);
    String r = sb.toString();
    return r;
    */
  }

  String getCommitLogAbsPath() {
    return PRE_COMMIT_PATH_PREFIX + "/" + getClientName() + ".log";
  }

  private void generateTmpAndDestDataPaths() {
    long ts = System.currentTimeMillis()/1000L;
    this.currTmpDataPath = this.hdfsPath+PRE_COMMIT_PATH_PREFIX+"/scribe.data."+ts;
    this.currDataPath = this.hdfsPath+COMMIT_PATH_PREFIX+"/scribe.data."+ts;
  }

  private String getTmpDataPathPattern() {
    return PRE_COMMIT_PATH_PREFIX+"/scribe.data.*";
  }
  
  private long getLatestOffsetFromKafka(String topic, int partition, long startTime) {
    TopicAndPartition tp = new TopicAndPartition(topic, partition);

    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();

    requestInfo.put(tp, new PartitionOffsetRequestInfo(startTime, 1));

    OffsetRequest req = new OffsetRequest(
        requestInfo, kafka.api.OffsetRequest.CurrentVersion(), getClientName());
    OffsetResponse resp;
    try{
      resp = consumer.getOffsetsBefore(req);
    } catch(Exception e){
      return 0;
    }
    
    if (resp.hasError()) {
      log.error("error when fetching offset: " + resp.errorCode(topic, partition)); //xxx
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
    FileSystem fs = fileSystemFactory.build(URI.create(this.hdfsPath));

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
      if (entry != null && entry.isCheckSumValid()) {
        String tmpDataFilePath = entry.getSrcPath(); 
        FileSystem fs = fileSystemFactory.build(URI.create(tmpDataFilePath));
        if (fs.exists(new Path(tmpDataFilePath))) {
          System.err.println("EXISTS");
          // mv to the dest
          commitData(tmpDataFilePath, entry.getDestPath());
        } else {
          System.err.println("DELETE");
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
    } catch (Exception e){
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
    log.info(" getLatestOffsetFromCommitLog >>>> " + offsetFromCommitLog); //xxx
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
    log.info(">> lastCommittedOffset: " + lastCommittedOffset); //xxx

    while (true) {
      log.info(">> offset: " + offset); //xxx
      FetchRequest req = new FetchRequestBuilder()
        .clientId(getClientName())
        .addFetch(topic, partition, offset, 100000)
        .build();
      FetchResponse resp = consumer.fetch(req);
      
      if (resp.hasError()) {
        //TODO: if we got an invalid offset, reset it by asking for the last element.
        // otherwise, find a new leader from the replica
        log.error("has error");  //xxx

        short code = resp.errorCode(topic, partition);
        log.error("Reason: " + code);
        if (code == ErrorMapping.OffsetOutOfRangeCode())  {
          // We asked for an invalid offset. For simple case ask for the last element to reset
          log.error("inside errormap");
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
            log.info("Found an old offset: " + currentOffset + "Expecting: " + offset);
            continue;
          }
          offset = messageAndOffset.nextOffset();
          ByteBuffer payload = messageAndOffset.message().payload();
          
          byte[] bytes = new byte[payload.limit()];
          payload.get(bytes);
          log.info(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes));
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

    sc.setScribeCommitLogFactory(ScribeCommitLogFactory.instance(sc.getHdfsPath()+sc.getCommitLogAbsPath()));
    if(sc.getHdfsPath().isEmpty()){
      sc.setFileSystemFactory(FileSystemFactory.instance());
    }
    else{
      sc.setFileSystemFactory(HDFSFileSystemFactory.instance());
    }
    sc.init();
    sc.run();
  }

}
