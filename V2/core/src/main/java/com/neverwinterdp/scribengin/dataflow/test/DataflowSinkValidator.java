package com.neverwinterdp.scribengin.dataflow.test;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.tool.message.MessageExtractor;
import com.neverwinterdp.util.JSONSerializer;

//TODO: review and update the parameter description
abstract public class DataflowSinkValidator implements Runnable {
  @Parameter(names = "--sink-location", description = "The storage sink dataflowName, usually the database dataflowName or dir dataflowName of the storage")
  protected String sinkLocation = "build/storage" ;
  
  @Parameter(names = "--sink-name", required=true, description = "The storage sink dataflowName, usually the database dataflowName or dir dataflowName of the storage")
  protected String sinkName ;
  
  @Parameter(names = "--sink-expect-records", description = "The expect number of records in the sink")
  protected long    expectRecords = 100000;
  
  public void setExpectRecords(long num) { this.expectRecords = num ; }
  
  abstract public StorageDescriptor getSinkDescriptor() ;
  
  abstract public void init(ScribenginClient scribenginClient) throws Exception ;
  
  abstract public void run() ;
  
  abstract public void runInBackground() ;
  
  abstract public boolean waitForTermination() throws InterruptedException;
  
  abstract public boolean waitForTermination(long timeout) throws InterruptedException;
  
  abstract public void populate(DataflowTestReport report) ;
  
  static public class RecordMessageExtractor implements MessageExtractor {
    @Override
    public Message extract(byte[] message) {
      Record record = JSONSerializer.INSTANCE.fromBytes(message, Record.class) ;
      return MessageExtractor.DEFAULT_MESSAGE_EXTRACTOR.extract(record.getData()) ;
    }
  }
}
