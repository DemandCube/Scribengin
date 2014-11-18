package com.neverwinterdp.scribengin.source;

import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class UUIDSourceStream implements SourceStream{

  private String name;
  private LinkedList<Tuple> data;
  private int key;
  
  public UUIDSourceStream(){
    name = UUID.randomUUID().toString();
    data = new LinkedList<Tuple>();
    key = 0;
  }
  
  
  @Override
  public Tuple readNext() {
    Tuple t = new Tuple(Integer.toString(key), 
                      UUID.randomUUID().toString().getBytes(),
                      new CommitLogEntry(this.getName(), key, key));
    data.add(t);
    key++;
    
    return t;
  }
  
  @Override
  public Tuple readFromOffset(long startOffset, long endOffset) {
    if(startOffset < data.size()){
      return data.get((int)(startOffset));
    }
    else{
      return null;
    }
  }

  @Override
  public boolean openStream() {
    return true;
  }

  @Override
  public boolean closeStream() {
    return true;
  }

  @Override
  public boolean hasNext() {
    return true;
  }

  
  public int getNumTuples(){
    return data.size();
  }

  @Override
  public String getName() {
    return this.name;
  }

  
}
