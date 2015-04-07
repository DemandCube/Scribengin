package com.neverwinterdp.scribengin.dataflow.test;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.source.HDFSSource;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;
import com.neverwinterdp.tool.message.Message;
import com.neverwinterdp.tool.message.MessageExtractor;
import com.neverwinterdp.tool.message.MessageTracker;

public class DataflowHDFSSinkValidator extends DataflowSinkValidator {
  private FileSystem     fs;
  private MessageTracker messageTracker;
  
  @Override
  public StorageDescriptor getSinkDescriptor() {
    String location = sinkLocation + "/" + sinkName;
    StorageDescriptor storageDescriptor = new StorageDescriptor("HDFS", location) ;
    return storageDescriptor;
  }

  @Override
  public void init(ScribenginClient scribenginClient) throws Exception {
    fs = scribenginClient.getVMClient().getFileSystem();
  }

  @Override
  public void run() {
    messageTracker = new MessageTracker() ;
    MessageExtractor messageExtractor = MessageExtractor.DEFAULT_MESSAGE_EXTRACTOR ;
    try {
      HDFSSource source = new HDFSSource(fs, getSinkDescriptor()) ;
      SourceStream[] streams = source.getStreams();
      for(SourceStream selStream : streams) {
        SourceStreamReader streamReader = selStream.getReader("DataflowHDFSSinkValidator") ;
        Record record = null ;
        while((record = streamReader.next()) != null) {
          Message message = messageExtractor.extract(record.getData()) ;
          messageTracker.log(message);
        }
        streamReader.close();
      }
      messageTracker.optimize();
      messageTracker.dump(System.out);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void runInBackground() {
    throw new RuntimeException("Due to the nature of hdfs storage, this method is not avaialable") ;
  }

  @Override
  public boolean waitForTermination() throws InterruptedException {
    throw new RuntimeException("Due to the nature of hdfs storage, this method is not avaialable") ;
  }

  @Override
  public boolean waitForTermination(long timeout) throws InterruptedException {
    throw new RuntimeException("Due to the nature of hdfs storage, this method is not avaialable") ;
  }

  @Override
  public void populate(DataflowTestReport report) {
  }
}
