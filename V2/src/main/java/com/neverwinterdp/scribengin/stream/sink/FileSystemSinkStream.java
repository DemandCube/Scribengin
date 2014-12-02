package com.neverwinterdp.scribengin.stream.sink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.stream.streamdescriptor.FileSinkStreamDescriptor;
import com.neverwinterdp.scribengin.stream.streamdescriptor.StreamDescriptor;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class FileSystemSinkStream implements SinkStream{
  LinkedList<Tuple> buffer; 
  FileSystem fs;
  String tmpFile;
  String commitDir;
  String name;
  
  public FileSystemSinkStream(){
    this("./tmp","./commit", "FileSystemSinkStream-"+UUID.randomUUID().toString());
  }
  
  public FileSystemSinkStream(String tmpFile, String commitDir){
    this(tmpFile, commitDir, "FileSystemSinkStream-"+UUID.randomUUID().toString());
  }
  
  public FileSystemSinkStream(FileSinkStreamDescriptor sDesc){
    this(sDesc.getFilename(), sDesc.getDir(), sDesc.getName());
  }
  
  public FileSystemSinkStream(String tmpFile, String commitDir, String name){
    this.name = name;
    this.buffer = new LinkedList<Tuple>();
    this.tmpFile = tmpFile;
    this.commitDir = commitDir;
    try {
      fs = FileSystem.get(new Configuration());
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    try {
      if(!fs.isDirectory(new Path(this.commitDir))){
        fs.mkdirs(new Path(this.commitDir));
      }
    } catch (IllegalArgumentException | IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public boolean prepareCommit() {
    return true;
  }

  @Override
  public boolean commit() {
    //Nothing to do
    if(buffer.isEmpty()){
      return true;
    }
    
    //Get all the bytes
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
    for(Tuple t: buffer){
      try {
        outputStream.write( t.getData());
        outputStream.write( "\n".getBytes());
      } catch (IOException e) {
        e.printStackTrace();
        return false;
      }
    }

    //Write to temp file
    try {
      FSDataOutputStream out = this.fs.create(new Path(this.tmpFile), true);
      out.write(outputStream.toByteArray());
      out.flush();
      out.close();
    } catch (IllegalArgumentException | IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  @Override
  public boolean clearBuffer() {
    buffer.clear();
    return true;
  }

  @Override
  public boolean completeCommit() {
    //Nothing was done, shouldn't need to do anything now
    if(buffer.isEmpty()){
      return true;
    }
    String filename = this.getNextAvailableCommitFile();
    Path tmpFilePath = new Path(this.tmpFile);
    
    try {
      if(!fs.exists(tmpFilePath)){
        buffer.clear();
        return true;
      }
      this.fs.rename(new Path(this.tmpFile), new Path(this.commitDir+"/"+filename));
      buffer.clear();
    } catch (IllegalArgumentException | IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  //This is where a SinkPartitioner should be implemented
  private String getNextAvailableCommitFile() {
    FileStatus[] status;
    try {
      status = this.fs.listStatus(new Path(this.commitDir));
    } catch (IllegalArgumentException | IOException e) {
      e.printStackTrace();
      return null;
    }
    
    if(status.length < 1){
      return "0";
    }
    
    int largestFileName = 0;
    
    for (int i=0;i<status.length;i++){
      //System.err.println(status[i].getPath().getName());
      int fileName = Integer.parseInt(status[i].getPath().getName());
      if(status[i].isFile() && fileName > largestFileName){
        largestFileName = fileName;
      }
    }
    return Integer.toString(++largestFileName);
  }

  @Override
  public boolean bufferTuple(Tuple t) {
    return buffer.add(t);
  }

  @Override
  public boolean rollBack() {
    try {
      buffer.clear();
      Path tmpFilePath = new Path(this.tmpFile);
      if(fs.exists(tmpFilePath)){
        return this.fs.delete(tmpFilePath, true);
      }
      return true;
    } catch (IllegalArgumentException | IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public void setSinkPartitioner(SinkPartitioner sp) {
    
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public long getBufferSize() {
    return buffer.size();
  }
  
  public LinkedList<Tuple> getBuffer(){
    return this.buffer;
  }
  
  @Override
  public StreamDescriptor getStreamDescriptor() {
    return new FileSinkStreamDescriptor(this.getName(), this.commitDir, this.tmpFile);
  }

}
