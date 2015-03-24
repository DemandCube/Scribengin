package com.neverwinterdp.scribengin.storage.sink;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.hdfs.sink.HDFSSink;
import com.neverwinterdp.scribengin.storage.kafka.sink.KafkaSink;
import com.neverwinterdp.scribengin.storage.s3.S3Client;
import com.neverwinterdp.scribengin.storage.s3.sink.S3Sink;

@Singleton
public class SinkFactory {
  @Inject
  private FileSystem fs;
  
  public SinkFactory() {
  }
  
  public SinkFactory(FileSystem fs) {
    this.fs = fs;
  }
  
  public Sink create(StorageDescriptor descriptor) throws Exception {
    if("hdfs".equalsIgnoreCase(descriptor.getType())) {
      return new HDFSSink(fs, descriptor);
    } else if("kafka".equalsIgnoreCase(descriptor.getType())) {
      return new KafkaSink(descriptor);
    }else if("s3".equalsIgnoreCase(descriptor.getType())) {      
      return new S3Sink(descriptor);
    }
    throw new Exception("Unknown source type " + descriptor.getType());
  }
  
  public Sink create(StreamDescriptor descriptor) throws Exception {
    if("hdfs".equalsIgnoreCase(descriptor.getType())) {
      return new HDFSSink(fs, descriptor);
    } else if("kafka".equalsIgnoreCase(descriptor.getType())) {
      return new KafkaSink(descriptor);
    }else if("s3".equalsIgnoreCase(descriptor.getType())) {      
      return new S3Sink(descriptor);
    }
    throw new Exception("Unknown source type " + descriptor.getType());
  }
}