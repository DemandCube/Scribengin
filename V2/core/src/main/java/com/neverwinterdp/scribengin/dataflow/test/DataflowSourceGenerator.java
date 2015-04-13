package com.neverwinterdp.scribengin.dataflow.test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.tool.message.MessageGenerator;
import com.neverwinterdp.util.JSONSerializer;

//TODO: Review and update the parameter description
abstract public class DataflowSourceGenerator implements Runnable {
  @Parameter(names = "--source-location", description = "The storage source dataflowName, usually the database dataflowName or dir dataflowName of the storage")
  protected String sourceLocation = "build/storage";
  
  @Parameter(names = "--source-name", description = "The storage source dataflowName, usually the database dataflowName or dir dataflowName of the storage")
  protected String sourceName = "hello";
  
  @Parameter(names = "--source-drop-if-exists", description = "The maximum number of record per stream")
  protected boolean dropIfExists;
  
  @Parameter(names = "--source-num-of-stream", description = "The number of stream for the source")
  protected int    numberOfStream = 2;
  
  @Parameter(names = "--source-record-size", description = "The maximum number of record per stream")
  protected int    recordSize;
  
  @Parameter(names = "--source-max-records-per-stream", description = "The maximum number of record per stream")
  protected int    maxRecordsPerStream;
  
  @Parameter(names = "--source-write-period", description = "The period that the generator should produce a record")
  protected long    writePeriod;
  
  @Parameter(names = "--source-max-duration", description = "The maximum number of record per stream")
  protected long    maxDuration;

  private   long    numberOfGeneratedRecords ;
  
  public String getSourceName() { return sourceName; }

  public int getNumberOfStream() { return numberOfStream; }

  public int getMaxRecordsPerStream() { return maxRecordsPerStream; }

  public long getWritePeriod() { return writePeriod; }

  public long getMaxDuration() { return maxDuration; }

  public long getNumberOfGeneratedRecords() { return numberOfGeneratedRecords; }
  public void setNumberOfGeneratedRecords(long numberOfGeneratedRecords) {
    this.numberOfGeneratedRecords = numberOfGeneratedRecords;
  }

  abstract public StorageDescriptor getSourceDescriptor();
  
  abstract public void init(ScribenginClient scribenginClient) throws Exception ;
  
  abstract public void run() ;
  
  abstract public void runInBackground() ;
  
  abstract public void populate(DataflowTestReport report) ;

  static public class RecordMessageGenerator implements MessageGenerator {
    MessageGenerator defaultMessageGenerator = new MessageGenerator.DefaultMessageGenerator() ;
    static public AtomicLong idTracker = new AtomicLong() ;
    
    public byte[] nextMessage(int partition, int messageSize) {
      return JSONSerializer.INSTANCE.toBytes(nextRecord(partition, messageSize));
    }
    
    public Record nextRecord(int partition, int messageSize) {
      byte[] messagePayload = defaultMessageGenerator.nextMessage(partition, messageSize);
      String key = "partition=" + partition + ",id=" + idTracker.getAndIncrement();
      return new Record(key, messagePayload);
    }

    @Override
    public Map<Integer, AtomicInteger> getMessageTrackers() {
      return defaultMessageGenerator.getMessageTrackers();
    }
  }
}