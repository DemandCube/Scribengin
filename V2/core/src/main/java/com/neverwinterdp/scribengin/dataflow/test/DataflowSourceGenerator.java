package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;

abstract public class DataflowSourceGenerator implements Runnable {
  @Parameter(names = "--source-name", description = "The storage source name, usually the database name or dir name of the storage")
  protected String sourceName = "hello";
  
  @Parameter(names = "--source-num-of-stream", description = "The number of stream for the source")
  protected int    numberOfStream ;
  
  @Parameter(names = "--source-max-records-per-stream", description = "The maximum number of record per stream")
  protected int    maxRecordsPerStream;
  
  @Parameter(names = "--source-write-period", description = "The period that the generator should produce a record")
  protected long    writePeriod;
  
  @Parameter(names = "--source-max-duration", description = "The maximum number of record per stream")
  protected long    maxDuration;
  
  public String getSourceName() { return sourceName; }

  public int getNumberOfStream() { return numberOfStream; }

  public int getMaxRecordsPerStream() { return maxRecordsPerStream; }

  public long getWritePeriod() { return writePeriod; }

  public long getMaxDuration() { return maxDuration; }

  abstract public StorageDescriptor getSourceDescriptor();
  
  abstract public void init(ScribenginClient scribenginClient) ;
  
  abstract public void run() ;
  
  abstract public void runInBackground() ;
  
  abstract public void populate(DataflowTestReport report) ;
}
