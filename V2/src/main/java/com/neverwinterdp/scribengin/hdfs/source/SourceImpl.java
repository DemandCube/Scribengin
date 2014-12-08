package com.neverwinterdp.scribengin.hdfs.source;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.hdfs.HDFSUtil;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceDescriptor;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.scribengin.source.SourceStreamDescriptor;

/**
 * @author Tuan Nguyen
 */
public class SourceImpl implements Source {
  private FileSystem fs;
  private SourceDescriptor descriptor ;
  private Map<Integer,SourceStreamImpl> streams = new LinkedHashMap<Integer, SourceStreamImpl>();
  
  public SourceImpl(FileSystem fs, String location) throws Exception {
    this(fs, new SourceDescriptor("HDFS", location));
  }
  
  public SourceImpl(FileSystem fs, SourceDescriptor descriptor) throws Exception {
    this.fs = fs;
    this.descriptor = descriptor ;
    Path fsLoc = new Path(descriptor.getLocation());
    if(!fs.exists(fsLoc)) {
      throw new Exception("location " + descriptor.getLocation() + " does not exist!") ;
    }
    
    FileStatus[] status = fs.listStatus(new Path(descriptor.getLocation())) ;
    for(int i = 0; i < status.length; i++) {
      SourceStreamDescriptor sDescriptor = new SourceStreamDescriptor();
      sDescriptor.setType(descriptor.getType());
      sDescriptor.setLocation(status[i].getPath().toString());
      sDescriptor.setId(HDFSUtil.getStreamId(status[i].getPath()));
      SourceStreamImpl stream = new SourceStreamImpl(fs, sDescriptor);
      streams.put(sDescriptor.getId(), stream);
    }
  }
  
  public SourceDescriptor getDescriptor() { return descriptor; }

  public SourceStream   getStream(int id) { return streams.get(id) ; }
  
  public SourceStream   getStream(SourceStreamDescriptor descriptor) { 
    return streams.get(descriptor.getId()) ; 
  }
  
  public SourceStream[] getStreams() {
    SourceStream[] array = new SourceStream[streams.size()];
    return streams.values().toArray(array);
  }
  
  static public List<Record> generate(int size, int dataSize) {
    List<Record> records = new ArrayList<Record>() ;
    for(int i = 0; i < size; i++) {
      records.add(new Record()) ;
    }
    return records ;
  }
}
