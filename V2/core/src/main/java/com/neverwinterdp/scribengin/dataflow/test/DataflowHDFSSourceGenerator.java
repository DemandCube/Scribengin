package com.neverwinterdp.scribengin.dataflow.test;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.ScribenginClient;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class DataflowHDFSSourceGenerator extends DataflowSourceGenerator {
  private FileSystem fs  ;
  
  //TODO: replace those parameters by the source parameter in the DataflowSourceGenerator 
  private int numOfStream = 5;
  private int numOfBufferPerStream = 3;
  private int numOfRecordPerBuffer = 10;
  
  
  @Override
  public StorageDescriptor getSourceDescriptor() {
    String location = sourceLocation + "/" + sourceName ;
    StorageDescriptor storageDescriptor = new StorageDescriptor("HDFS", location) ;
    return storageDescriptor;
  }

  @Override
  public void init(ScribenginClient scribenginClient) throws Exception {
    fs = scribenginClient.getVMClient().getFileSystem();
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

  //TODO: need to generate the Record properly so it can be used with the check tool.
  //Look into the kafka source generator to see how to make it 
  void generateStream(Sink sink) throws Exception {
    SinkStream stream = sink.newStream();
    SinkStreamWriter writer = stream.getWriter();
    for(int i = 0; i < numOfBufferPerStream; i++) {
      for(int j = 0; j < numOfRecordPerBuffer; j ++) {
        String key = "stream=" + stream.getDescriptor().getId() +",buffer=" + i + ",record=" + j;
        writer.append(Record.create(key, key));
      }
      writer.commit();
    }
    writer.close();
  }
}