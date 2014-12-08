package com.neverwinterdp.scribengin.hdfs.sink;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;

public class SinkStreamImpl implements SinkStream {
  private FileSystem fs ;
  private SinkStreamDescriptor descriptor;
  
  public SinkStreamImpl(FileSystem fs, SinkStreamDescriptor descriptor) {
    this.fs = fs;
    this.descriptor = descriptor;
  }
  
  public SinkStreamImpl(FileSystem fs, Path path) {
    this.fs = fs ;
    descriptor = new SinkStreamDescriptor();
    descriptor.setLocation(path.toString());
    descriptor.setId(HDFSUtil.getStreamId(path)) ;
  }
  
  
  public SinkStreamDescriptor getDescriptor() { return this.descriptor ; }
  
  synchronized public void delete() throws Exception {
  }
  
  @Override
  synchronized public SinkStreamWriter getWriter() throws IOException {
    return new SinkStreamWriterImpl(fs, descriptor.getLocation());
  }
  
  synchronized public void fsCheck() throws Exception {
    //TODO: Need to discuss more how this should work. Basically the check should go throug the buffer dir, 
    //delete or merge the complete buffer. 
  }
}