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
    this(fs, new SinkDescriptor("HDFS", location));
  }
  
  public SinkImpl(FileSystem fs, SinkStreamDescriptor streamDescriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    this(fs, getSinkDescriptor(streamDescriptor));
  }
  
  public SinkImpl(FileSystem fs, SinkDescriptor descriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    this.fs = fs;
    this.descriptor = descriptor;
    
    Path fsLoc = new Path(descriptor.getLocation());
    if(!fs.exists(fsLoc)) fs.mkdirs(fsLoc) ;
    FileStatus[] status = fs.listStatus(fsLoc) ;
    for(int i = 0; i < status.length; i++) {
      SinkStreamImpl stream = new SinkStreamImpl(fs, status[i].getPath());
      streams.put(stream.getDescriptor().getId(), stream);
    }
  }
  
  public SinkDescriptor getDescriptor() { return this.descriptor; }
  
  public SinkStream  getStream(SinkStreamDescriptor descriptor) throws Exception {
    SinkStream stream = streams.get(descriptor.getId());
    if(stream == null) {
      throw new Exception("Cannot find the stream " + descriptor.getId()) ;
    }
    return stream ;
  }
  
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
  synchronized public SinkStream newStream() throws IOException {
    int id = idTracker++;
    String location = descriptor.getLocation() + "/stream-" + id;
    SinkStreamDescriptor streamDescriptor = new SinkStreamDescriptor("HDFS", id, location) ;
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
  
  static SinkDescriptor getSinkDescriptor(SinkStreamDescriptor streamDescriptor) {
    String location = streamDescriptor.getLocation();
    location = location.substring(0, location.lastIndexOf('/'));
    SinkDescriptor descriptor = new SinkDescriptor(streamDescriptor.getType(), location);
    return descriptor;
  }
}