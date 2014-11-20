package com.neverwinterdp.scribengin.stream.source;

import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class UUIDSourceStream implements SourceStream{

  private String name;
  private LinkedList<Tuple> data;
  private int currentOffset;
  private int lastCommitted;
  
  public UUIDSourceStream(){
    name = UUID.randomUUID().toString();
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
  
  /*
  @Override
  public Tuple readFromOffset(long startOffset, long endOffset) {
    if(startOffset < data.size()){
      return data.get((int)(startOffset));
    }
    else{
      return null;
    }
  }
   */

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
  public boolean clearCommit() {
    this.currentOffset = this.lastCommitted;
    return false;
  }


  @Override
  public boolean updateOffSet() {
    this.lastCommitted = this.currentOffset; 
    return true;
  }


  
}
