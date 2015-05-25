package com.neverwinterdp.scribengin.storage.hdfs.sink;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkStream;

public class HDFSSink implements Sink {
  private FileSystem fs;
  private StorageDescriptor descriptor;

  private int idTracker = 0;
  private LinkedHashMap<Integer, HDFSSinkStream> streams = new LinkedHashMap<Integer, HDFSSinkStream>() ;
  
  public HDFSSink(FileSystem fs, String location) throws FileNotFoundException, IllegalArgumentException, IOException {
    this(fs, new StorageDescriptor("HDFS", location));
  }
  
  public HDFSSink(FileSystem fs, StreamDescriptor streamDescriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    this(fs, getSinkDescriptor(streamDescriptor));
  }
  
  public HDFSSink(FileSystem fs, StorageDescriptor descriptor) throws FileNotFoundException, IllegalArgumentException, IOException {
    this.fs = fs;
    this.descriptor = descriptor;
    
    Path fsLoc = new Path(descriptor.getLocation());
    if(!fs.exists(fsLoc)) fs.mkdirs(fsLoc) ;
    FileStatus[] status = fs.listStatus(fsLoc) ;
    for(int i = 0; i < status.length; i++) {
      HDFSSinkStream stream = new HDFSSinkStream(fs, status[i].getPath());
      streams.put(stream.getDescriptor().getId(), stream);
    }
  }
  
  public StorageDescriptor getDescriptor() { return this.descriptor; }
  
  public SinkStream  getStream(StreamDescriptor descriptor) throws Exception {
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
    StreamDescriptor streamDescriptor = new StreamDescriptor("HDFS", id, location) ;
    HDFSSinkStream stream = new HDFSSinkStream(fs, streamDescriptor);
    streams.put(streamDescriptor.getId(), stream) ;
    return stream;
  }

  @Override
  public void close() throws Exception  { 
  }
  
  public void fsCheck() throws Exception {
    //TODO: this method should go through all the sink stream and call fsCheck of each stream
  }
  
  static StorageDescriptor getSinkDescriptor(StreamDescriptor streamDescriptor) {
    String location = streamDescriptor.getLocation();
    location = location.substring(0, location.lastIndexOf('/'));
    StorageDescriptor descriptor = new StorageDescriptor(streamDescriptor.getType(), location);
    return descriptor;
  }
}