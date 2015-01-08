package com.neverwinterdp.scribengin.sink;

import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.scribengin.hdfs.sink.SinkImpl;

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
      return new SinkImpl(fs, descriptor);
    }
    throw new Exception("Unknown source type " + descriptor.getType());
  }
  
  public Sink create(SinkStreamDescriptor descriptor) throws Exception {
    if("hdfs".equalsIgnoreCase(descriptor.getType())) {
      return new SinkImpl(fs, descriptor);
    }
    throw new Exception("Unknown source type " + descriptor.getType());
  }
}