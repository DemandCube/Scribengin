package com.neverwinterdp.scribengin.storage.hdfs;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class HDFSSourceGenerator {
  private int numOfStream = 5;
  private int numOfBufferPerStream = 3;
  private int numOfRecordPerBuffer = 10;
  
  public void generateSource(FileSystem fs, String sourceDir) throws Exception {
    SinkFactory sinkFactory = new SinkFactory(fs);
    StorageDescriptor sinkDescriptor = new StorageDescriptor("hdfs", sourceDir);
    Sink sink = sinkFactory.create(sinkDescriptor);;
    for(int i = 0; i < numOfStream; i++) {
      generateStream(sink);
    }
  }
  
  void generateStream(Sink sink) throws Exception {
    SinkStream stream = sink.newStream();
    SinkStreamWriter writer = stream.getWriter();
    for(int i = 0; i < numOfBufferPerStream; i++) {
      for(int j = 0; j < numOfRecordPerBuffer; j ++) {
        String key = "stream:" + stream.getDescriptor().getId() +",buffer:" + i + ",record:" + j;
 
        writer.append(Record.create(key, key));
      }
      writer.commit();
    }
    writer.close();
  }
}
