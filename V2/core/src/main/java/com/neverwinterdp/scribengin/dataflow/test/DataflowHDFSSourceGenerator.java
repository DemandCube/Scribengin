package com.neverwinterdp.scribengin.dataflow.test;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class DataflowHDFSSourceGenerator extends DataflowSourceGenerator {
  private FileSystem fs  ;
  private RecordMessageGenerator recordGenerator = new RecordMessageGenerator() ;
  
  private int numOfStream;
  private int numOfBufferPerStream;
  private int numOfRecordPerBuffer;
  
  @Override
  public StorageDescriptor getSourceDescriptor() {
    String location = sourceLocation + "/" + sourceName ;
    StorageDescriptor storageDescriptor = new StorageDescriptor("HDFS", location) ;
    return storageDescriptor;
  }

  @Override
  public void init(ScribenginClient scribenginClient) throws Exception {
    fs = scribenginClient.getVMClient().getFileSystem();
    numOfStream = numberOfStream;
    numOfBufferPerStream=1;
    numOfRecordPerBuffer = maxRecordsPerStream/numOfBufferPerStream;
  }

  @Override
  public void run() {
    try {
      String location = sourceLocation + "/" + sourceName ;
      generateSource(fs, location);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void runInBackground() {
    throw new RuntimeException("this method is not supported for the hdfs due to the nature of the storage") ;
  }

  @Override
  public void populate(DataflowTestReport report) {
  }
  
  void generateSource(FileSystem fs, String sourceDir) throws Exception {
    SinkFactory sinkFactory = new SinkFactory(fs);
    StorageDescriptor sinkDescriptor = new StorageDescriptor("hdfs", sourceDir);
    Sink sink = sinkFactory.create(sinkDescriptor);;
    for(int i = 0; i < numOfStream; i++) {
      generateStream(sink);
    }
  }

  void generateStream(Sink sink) throws Exception {
    SinkStream stream = sink.newStream();
    int partition = stream.getDescriptor().getId() ;
    SinkStreamWriter writer = stream.getWriter();
    for(int i = 0; i < numOfBufferPerStream; i++) {
      for(int j = 0; j < numOfRecordPerBuffer; j ++) {
        writer.append(recordGenerator.nextRecord(partition, recordSize));
      }
      writer.commit();
    }
    writer.close();
  }
}