package com.neverwinterdp.scribengin.hdfs.sink;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.Record;
import com.neverwinterdp.scribengin.sink.SinkStreamWriter;
import com.neverwinterdp.util.JSONSerializer;
import com.neverwinterdp.vm.environment.yarn.HDFSUtil;

public class HDFSSinkStreamWriter implements SinkStreamWriter {
  private FileSystem fs;
  private String location ;
  private String bufferLocation ;
  private SinkBuffer currentBuffer ;
  
  public HDFSSinkStreamWriter(FileSystem fs, String location) throws IOException {
    this.fs = fs;
    this.location = location;
    this.bufferLocation = location + "/.buffer" + UUID.randomUUID().toString();
    Path bufferPath = new Path(bufferLocation);
    if(!fs.exists(bufferPath)) fs.mkdirs(bufferPath);
    this.currentBuffer = nextSinkBuffer();
  }
  
  @Override
  synchronized public boolean append(Record record) throws Exception {
    currentBuffer.append(record);
    return true;
  }

  @Override
  public boolean rollback() throws Exception {
    currentBuffer.rollback();
    return true;
  }

  @Override
  public boolean prepareCommit() throws Exception {
    //TODO: reimplement correctly 2 phases commit
    return true;
  }

  @Override
  public boolean completeCommit() throws Exception {
  //TODO: reimplement correctly 2 phases commit
    currentBuffer.commit();
    currentBuffer = nextSinkBuffer();
    return true;
  }
  
  @Override
  synchronized public boolean commit() throws Exception {
    try {
    prepareCommit();
    completeCommit();
    } catch(Exception ex) {
      rollback();
      throw ex;
    }
    return true;
  }
  
  @Override
  synchronized public boolean close() throws Exception {
    if(currentBuffer.count > 0) {
      currentBuffer.commit();
    } else {
      currentBuffer.delete();
    }
    Path datDestination = new Path(location + "/data-" + UUID.randomUUID().toString() + ".dat");
    FileStatus[] status = fs.listStatus(new Path(bufferLocation));
    if(status.length == 0) {
      fs.delete(new Path(this.bufferLocation), true);
      return false;
    } else {
      Path[] bufferSrc = new Path[status.length];
      for(int i = 0; i < bufferSrc.length; i++) {
        bufferSrc[i] = status[i].getPath();
      }
      HDFSUtil.concat(fs, datDestination, bufferSrc, true);
      fs.delete(new Path(this.bufferLocation), true);
    }
    return true;
  }
  
  private SinkBuffer nextSinkBuffer() throws IOException {
    int idx = 0;
    if(currentBuffer != null) {
      idx = currentBuffer.index + 1 ;
    }
    SinkBuffer buffer = new SinkBuffer(idx) ;
    return buffer;
  }
  
  class SinkBuffer {
    private int  index;
    private Path writingPath;
    private Path completePath;
    private FSDataOutputStream output;
    private int count = 0 ;
    
    public SinkBuffer(int index) throws IOException {
      this.index = index;
      writingPath = new Path(bufferLocation + "/buffer-" + index + ".writing") ;
      completePath = new Path(bufferLocation + "/buffer-" + index + ".complete") ;
      output = fs.create(writingPath) ;
    }
    
    public void append(Record record) throws IOException {
      byte[] bytes = JSONSerializer.INSTANCE.toBytes(record) ;
      output.writeInt(bytes.length);
      output.write(bytes);
      count++;
    }
    
    public void delete() throws IOException {
      output.close();
      fs.delete(writingPath, true) ;
    }
    
    public void rollback() throws IOException {
      output.close();
      output = fs.create(writingPath, true) ;
      count = 0;
    }
    
    public void commit() throws IOException {
      output.close();
      fs.rename(writingPath, completePath);
    }
  }
}