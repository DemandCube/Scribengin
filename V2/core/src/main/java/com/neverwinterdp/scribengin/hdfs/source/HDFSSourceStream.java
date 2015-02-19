package com.neverwinterdp.scribengin.hdfs.source;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;
import com.neverwinterdp.scribengin.source.SourceStreamReader;

public class HDFSSourceStream implements SourceStream {
  private FileSystem fs ;
  private SourceStreamDescriptor descriptor ;
  
  public HDFSSourceStream(FileSystem fs, SourceStreamDescriptor descriptor) {
    this.fs = fs;
    this.descriptor = descriptor;
  }
  
  public SourceStreamDescriptor getDescriptor() { return descriptor ; }
  
  @Override
  public SourceStreamReader getReader(String name) throws FileNotFoundException, IllegalArgumentException, IOException {
    return new HDFSSourceStreamReader(name, fs, descriptor) ;
  }

}
