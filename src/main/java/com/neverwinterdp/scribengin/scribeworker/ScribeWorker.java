package com.neverwinterdp.scribengin.scribeworker;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
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

import com.neverwinterdp.scribengin.commitlog.AbstractScribeCommitLogFactory;
import com.neverwinterdp.scribengin.commitlog.ScribeCommitLog;
import com.neverwinterdp.scribengin.commitlog.ScribeCommitLogFactory;
import com.neverwinterdp.scribengin.commitlog.ScribeLogEntry;
import com.neverwinterdp.scribengin.config.ScribeWorkerConfig;
import com.neverwinterdp.scribengin.filesystem.AbstractFileSystemFactory;
import com.neverwinterdp.scribengin.filesystem.FileSystemFactory;
import com.neverwinterdp.scribengin.filesystem.HDFSFileSystemFactory;
import com.neverwinterdp.scribengin.writer.StringRecordWriter;

public class ScribeWorker {
  private static final Logger log = Logger.getLogger(ScribeWorker.class.getName());
  private String PRE_COMMIT_PATH_PREFIX;
  private String COMMIT_PATH_PREFIX;
  private String currTmpDataPath;
  private String currDataPath;
  private AbstractScribeCommitLogFactory scribeCommitLogFactory;
  private AbstractFileSystemFactory fileSystemFactory;
  private String topic;
  private String leaderHost;
  private int leaderPort;
  private int partition;
  private long lastCommittedOffset;
  private long offset; // offset is on a per line basis. starts on the last valid offset
  private long commitCheckPointInterval; // ms
  private String hdfsPath="";
  private Timer checkPointIntervalTimer;
  private SimpleConsumer consumer;
  private Thread workerThread;
  
  
  public ScribeWorker(ScribeWorkerConfig c){
    this.PRE_COMMIT_PATH_PREFIX = c.PRE_COMMIT_PATH_PREFIX;
    this.COMMIT_PATH_PREFIX = c.COMMIT_PATH_PREFIX;
    this.topic = c.topic;
    this.leaderHost = c.leaderHost;
    this.leaderPort = c.leaderPort;
    this.partition = c.partition;
    this.commitCheckPointInterval = c.commitCheckPointInterval; // ms
    this.hdfsPath = c.hdfsPath;
    
    this.checkPointIntervalTimer = new Timer();
  }
  
  public Thread.State getState(){
    return workerThread.getState();
  }
  
  public void start(){
    setScribeCommitLogFactory(ScribeCommitLogFactory.instance(this.hdfsPath+getCommitLogAbsPath()));
    if(this.hdfsPath == null){
      setFileSystemFactory(FileSystemFactory.instance());
    }
    else{
      setFileSystemFactory(HDFSFileSystemFactory.instance());
    }
    
    consumer = new SimpleConsumer(
        this.leaderHost,
        this.leaderPort,
        10000,   // timeout
        64*1024, // buffersize
        getClientName() //clientID
        );

    scheduleCommitTimer();
    
    workerThread = new Thread() {
      public void run() {
        try{
          runWorkerLoop() ;
        }
          catch (Exception e) {
          e.printStackTrace();
        }
      }
    };
    workerThread.start();
  }
  
  
  public void stop(){
    try{
      checkPointIntervalTimer.cancel();
    } catch(Exception e){
      e.printStackTrace();
    }
    try{
      workerThread.interrupt();
    } catch(Exception e){
      e.printStackTrace();
    }
  }
  
  private void scheduleCommitTimer() {
    checkPointIntervalTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        commit();
      }
    }, commitCheckPointInterval);
  }
  
  private synchronized void commit() {
    //TODO: move from tmp to the actual partition.
    //log.info(">> committing");
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
  
  public void runWorkerLoop() throws IOException {
    generateTmpAndDestDataPaths();
    lastCommittedOffset = getLatestOffset(topic, partition, kafka.api.OffsetRequest.LatestTime());
    offset = lastCommittedOffset;
    log.info(">> lastCommittedOffset: " + lastCommittedOffset); //xxx

    while (true) {
      log.info(">> offset: " + offset); //xxx
      log.info(">> partition: "+ partition);
      log.info(">> topic: "+ topic);
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

      
      synchronized(this) {
        StringRecordWriter writer = new StringRecordWriter(currTmpDataPath);
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
      
      

      if (msgReadCnt == 0) {
        try {
          Thread.sleep(1000); //Didn't read anything, so go to sleep for awhile.
        } catch(InterruptedException e) {
        }
      }
    } // while
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
      log.error("error when fetching offset: " + resp.errorCode(topic, partition)); 
      return 0;
    }
    return resp.offsets(topic, partition)[0];
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
  
  
  private void generateTmpAndDestDataPaths() {
    long ts = System.currentTimeMillis()/1000L;
    this.currTmpDataPath = this.hdfsPath+PRE_COMMIT_PATH_PREFIX+"/scribe.data."+ts;
    this.currDataPath = this.hdfsPath+COMMIT_PATH_PREFIX+"/scribe.data."+ts;
  }
  
  
  private String getTmpDataPathPattern() {
    return PRE_COMMIT_PATH_PREFIX+"/scribe.data.*";
  }
  
  private String getClientName() {
    return "scribe_"+topic+"_"+partition;
  }
  
  String getCommitLogAbsPath() {
    return PRE_COMMIT_PATH_PREFIX + "/" + getClientName() + ".log";
  }
  
  public void setFileSystemFactory(AbstractFileSystemFactory factory) {
    fileSystemFactory = factory;
  }
  
  public void setScribeCommitLogFactory(AbstractScribeCommitLogFactory factory) {
    scribeCommitLogFactory = factory;
  }
}
