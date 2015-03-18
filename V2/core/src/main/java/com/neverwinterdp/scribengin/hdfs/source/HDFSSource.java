package com.neverwinterdp.scribengin.hdfs.source;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

/**
 * @author Tuan Nguyen
 */
public class HDFSSource implements Source {
  private FileSystem fs;
  private StorageDescriptor descriptor ;
  private Map<Integer,HDFSSourceStream> streams = new LinkedHashMap<Integer, HDFSSourceStream>();
  
  public HDFSSource(FileSystem fs, String location) throws Exception {
    this(fs, new StorageDescriptor("HDFS", location));
  }
  
  public HDFSSource(FileSystem fs, StreamDescriptor streamDescriptor) throws Exception {
    this(fs, getSourceDescriptor(streamDescriptor)) ;
  }
  
  public HDFSSource(FileSystem fs, StorageDescriptor descriptor) throws Exception {
    this.fs = fs;
    this.descriptor = descriptor ;
    Path fsLoc = new Path(descriptor.getLocation());
    if(!fs.exists(fsLoc)) {
      throw new Exception("location " + descriptor.getLocation() + " does not exist!") ;
    }
    
    FileStatus[] status = fs.listStatus(new Path(descriptor.getLocation())) ;
    for(int i = 0; i < status.length; i++) {
      StreamDescriptor sDescriptor = new StreamDescriptor();
      sDescriptor.setType(descriptor.getType());
      sDescriptor.setLocation(status[i].getPath().toString());
      sDescriptor.setId(HDFSUtil.getStreamId(status[i].getPath()));
      HDFSSourceStream stream = new HDFSSourceStream(fs, sDescriptor);
      streams.put(sDescriptor.getId(), stream);
    }
  }
  
  public StorageDescriptor getDescriptor() { return descriptor; }

  public SourceStream   getStream(int id) { return streams.get(id) ; }
  
  public SourceStream   getStream(StreamDescriptor descriptor) { 
    return streams.get(descriptor.getId()) ; 
  }
  
  public SourceStream[] getStreams() {
    SourceStream[] array = new SourceStream[streams.size()];
    return streams.values().toArray(array);
  }
  
  public void close() throws Exception {
  }
  
  static StorageDescriptor getSourceDescriptor(StreamDescriptor streamDescriptor) {
    StorageDescriptor descriptor = new StorageDescriptor();
    descriptor.setType(streamDescriptor.getType());
    String location = streamDescriptor.getLocation();
    location = location.substring(0, location.lastIndexOf('/'));
    descriptor.setLocation(location);
    return descriptor ;
  }
}
