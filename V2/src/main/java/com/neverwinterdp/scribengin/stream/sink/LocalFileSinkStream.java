package com.neverwinterdp.scribengin.stream.sink;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.stream.streamdescriptor.FileSinkStreamDescriptor;
import com.neverwinterdp.scribengin.stream.streamdescriptor.StreamDescriptor;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class LocalFileSinkStream implements SinkStream{
  
  LinkedList<Tuple> buffer;
  String dir;
  String name;
  
  public LocalFileSinkStream(){
    this("./tmp/");
  }
  
  public LocalFileSinkStream(String directoryName){
    buffer = new LinkedList<Tuple>();
    this.dir = directoryName;
    this.name = this.getClass().getSimpleName() +"-"+UUID.randomUUID().toString();
  }
  
  public LocalFileSinkStream(FileSinkStreamDescriptor sDesc){
    this(sDesc.getDir());
    this.name = sDesc.getName();
  }
  
  @Override
  public boolean prepareCommit() {
    File f = new File(this.dir);
    if(f.exists() && f.isDirectory()) { 
      return f.canWrite() && f.canRead();
    }
    else if(f.exists() && !f.isDirectory()) { 
      return false;
    }
    else{
      return f.mkdirs();
    }
  }

  @Override
  public boolean commit() {
    for(Tuple t: buffer){
      Writer writer = null;
      try {
          writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.dir+t.getKey())));
          writer.write(t.toString());
      } catch (IOException ex) {
        ex.printStackTrace();
        return false;
      } finally {
         try {
           writer.close();
         } catch (Exception ex) {
           ex.printStackTrace();
           return false;
         }
      }
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
    buffer.clear();
    return true;
  }

  @Override
  public boolean bufferTuple(Tuple t) {
    return buffer.add(t);
  }

  @Override
  public boolean rollBack() {
    boolean retVal = true;
    for(Tuple t: buffer){
      File f;
      try {
        f = new File(this.dir+t.getKey());
        if(f.exists()){
          f.delete();
        }
      } catch (Exception e) {
        e.printStackTrace();
        retVal = false;
      } finally {
         try {
         } catch (Exception e) {
           e.printStackTrace();
           retVal = false;
         }
      }
    }
    buffer.clear();
    return retVal;
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
    return this.buffer.size();
  }

  
  @Override
  public StreamDescriptor getStreamDescriptor() {
    return new FileSinkStreamDescriptor(this.getName(), this.dir , "");
  }
}
