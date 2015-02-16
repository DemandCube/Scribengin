package com.neverwinterdp.scribengin.sink;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.inMemory.sink.TestSink;
import com.neverwinterdp.scribengin.hdfs.sink.HDFSSink;
import com.neverwinterdp.scribengin.kafka.sink.KafkaSink;

@Singleton
public class SinkFactory {
  @Inject
  private FileSystem fs;
  
  public SinkFactory() {
  }
  
  public SinkFactory(FileSystem fs) {
    this.fs = fs;
  }
  
  public Sink create(SinkDescriptor descriptor) throws Exception {
    if("hdfs".equalsIgnoreCase(descriptor.getType())) {
      return new HDFSSink(fs, descriptor);
    } else if("kafka".equalsIgnoreCase(descriptor.getType())) {
      return new KafkaSink(descriptor);
    }
    throw new Exception("Unknown source type " + descriptor.getType());
  }
  
  public Sink create(SinkStreamDescriptor descriptor) throws Exception {
    if("hdfs".equalsIgnoreCase(descriptor.getType())) {
      return new HDFSSink(fs, descriptor);
    } else if("kafka".equalsIgnoreCase(descriptor.getType())) {
      return new KafkaSink(descriptor);
    }
    else if("test".equalsIgnoreCase(descriptor.getType())){
      return new TestSink(descriptor);
    }
    throw new Exception("Unknown source type " + descriptor.getType());
  }
}