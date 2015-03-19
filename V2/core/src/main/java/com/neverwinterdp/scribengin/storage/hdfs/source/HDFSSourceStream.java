package com.neverwinterdp.scribengin.storage.hdfs.source;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.scribengin.storage.source.SourceStreamReader;

public class HDFSSourceStream implements SourceStream {
  private FileSystem fs ;
  private StreamDescriptor descriptor ;
  
  public HDFSSourceStream(FileSystem fs, StreamDescriptor descriptor) {
    this.fs = fs;
    this.descriptor = descriptor;
  }
  
  public StreamDescriptor getDescriptor() { return descriptor ; }
  
  @Override
  public SourceStreamReader getReader(String name) throws FileNotFoundException, IllegalArgumentException, IOException {
    return new HDFSSourceStreamReader(name, fs, descriptor) ;
  }

}
