package com.neverwinterdp.scribengin.stream.source;

import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.stream.streamdescriptor.OffsetStreamDescriptor;
import com.neverwinterdp.scribengin.stream.streamdescriptor.StreamDescriptor;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class UUIDSourceStream implements SourceStream{

  private String name;
  private LinkedList<Tuple> data;
  private int currentOffset;
  private int lastCommitted;
  
  public UUIDSourceStream(){
    name = this.getClass().getSimpleName() +"-"+UUID.randomUUID().toString();
    data = new LinkedList<Tuple>();
    currentOffset = 0;
    lastCommitted = 0;
  }
  
  public LinkedList<Tuple> getData(){
    return data;
  }
  
  @Override
  public Tuple readNext() {
    if(currentOffset < data.size()){
      return data.get(currentOffset++);
    }
    
    Tuple t = new Tuple(Integer.toString(currentOffset), 
                      UUID.randomUUID().toString().getBytes(),
                      new CommitLogEntry(this.getName(), currentOffset, currentOffset));
    data.add(t);
    
    currentOffset++;
    return t;
  }
  

  @Override
  public boolean hasNext() {
    return true;
  }


  @Override
  public String getName() {
    return this.name;
  }


  @Override
  public boolean prepareCommit() {
    return true;
  }


  @Override
  public boolean commit() {
    this.lastCommitted = this.currentOffset; 
    return true;
  }


  @Override
  public boolean clearBuffer() {
    this.currentOffset = this.lastCommitted;
    return false;
  }


  @Override
  public boolean completeCommit() {
    this.lastCommitted = this.currentOffset; 
    return true;
  }

  @Override
  public boolean rollBack() {
    return true;
  }

  @Override
  public StreamDescriptor getStreamDescriptor() {
    return new OffsetStreamDescriptor(this.getName(), this.lastCommitted, this.currentOffset);
  }


  
}
