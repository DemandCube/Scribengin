package com.neverwinterdp.scribengin.storage.hdfs.sink;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;
import com.neverwinterdp.scribengin.storage.sink.SinkStreamWriter;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSSinkStream implements SinkStream {
  private FileSystem fs ;
  private StreamDescriptor descriptor;
  
  
  public HDFSSinkStream(FileSystem fs, Path path) throws IOException {
    this.fs = fs ;
    descriptor = new StreamDescriptor("HDFS", HDFSUtil.getStreamId(path), path.toString());
    init();
  }
  
  public HDFSSinkStream(FileSystem fs, StreamDescriptor descriptor) throws IOException {
    this.fs = fs;
    this.descriptor = descriptor;
    init() ;
  }
  
  private void init() throws IOException {
    Path path = new Path(descriptor.getLocation()) ;
    if(!fs.exists(path)) fs.mkdirs(path);
  }
  
  public StreamDescriptor getDescriptor() { return this.descriptor ; }
  
  synchronized public void delete() throws Exception {
  }
  
  @Override
  synchronized public SinkStreamWriter getWriter() throws IOException {
    return new HDFSSinkStreamWriter(fs, descriptor.getLocation());
  }
  
  synchronized public void fsCheck() throws Exception {
    //TODO: Need to discuss more how this should work. Basically the check should go throug the buffer dir, 
    //delete or merge the complete buffer. 
  }
}