package com.neverwinterdp.scribengin.storage.s3;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;

public class S3SourceGenerator {
  private int numOfStream; // number of files
  private int numOfBufferPerStream =1; 
  private int numOfRecordPerBuffer;
  
  public void generateSource(S3Client s3Client, String bucketName, int numStreams, int numRecordsPerStream) throws Exception {
    this.numOfStream= numStreams;
    this.numOfRecordPerBuffer= numRecordsPerStream;
    
    String folderName = "sourcetest-0";
    StorageDescriptor descriptor = new StorageDescriptor("s3", bucketName);
    descriptor.attribute("s3.bucket.name", bucketName);
    descriptor.attribute("s3.storage.path", folderName);

    SinkFactory sinkFactory = new SinkFactory(s3Client);
    Sink sink = sinkFactory.create(descriptor);;
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
