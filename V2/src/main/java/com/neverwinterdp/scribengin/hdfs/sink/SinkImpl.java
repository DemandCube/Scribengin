package com.neverwinterdp.scribengin.hdfs.sink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkStream;
import com.neverwinterdp.scribengin.sink.SinkStreamDescriptor;

public class SinkImpl implements Sink {
  private int idTracker = 0;
  
  private FileSystem fs;
  private SinkDescriptor descriptor;
  
  private LinkedHashMap<Integer, SinkStreamImpl> streams = new LinkedHashMap<Integer, SinkStreamImpl>() ;
  
  public SinkImpl(FileSystem fs, String location) throws FileNotFoundException, IllegalArgumentException, IOException {
    this.fs = fs;
    descriptor = new SinkDescriptor();
    descriptor.setLocation(location);
    descriptor.setType("HDFS");
    
    Path fsLoc = new Path(location);
    if(!fs.exists(fsLoc)) fs.mkdirs(fsLoc) ;
    FileStatus[] status = fs.listStatus(new Path(location)) ;
    for(int i = 0; i < status.length; i++) {
      SinkStreamImpl stream = new SinkStreamImpl(fs, status[i].getPath());
      streams.put(stream.getDescriptor().getId(), stream);
    }
  }
  
  public SinkDescriptor getDescriptor() { return this.descriptor; }
  
  synchronized public SinkStream[] getStreams() {
    SinkStream[] array = new SinkStream[streams.size()] ;
    streams.values().toArray(array) ;
    return array;
  }

  @Override
  synchronized public void delete(SinkStream stream) throws Exception {
    SinkStream foundStream = streams.remove(stream.getDescriptor().getId()) ;
    if(foundStream == null) {
      throw new Exception("Cannot find the stream " + stream.getDescriptor().getId()) ;
    }
  }
  
  @Override
  synchronized public SinkStream newStream() {
    SinkStreamDescriptor streamDescriptor = new SinkStreamDescriptor() ;
    streamDescriptor.setId(idTracker++);
    streamDescriptor.setLocation(descriptor.getLocation() + "/stream-" + streamDescriptor.getId());
    SinkStreamImpl stream = new SinkStreamImpl(fs, streamDescriptor);
    streams.put(streamDescriptor.getId(), stream) ;
    return stream;
  }

  @Override
  public void close() throws Exception  { 
  }
  
  public void fsCheck() throws Exception {
    //TODO: this method should go through all the sink stream and call fsCheck of each stream
  }
}
