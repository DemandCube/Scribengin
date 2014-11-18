package com.neverwinterdp.scribengin.source;

import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.commitlog.CommitLogEntry;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class UUIDSourceStream implements SourceStream{

  private String name;
  private LinkedList<byte[]> data;
  
  public UUIDSourceStream(){
    name = UUID.randomUUID().toString();
    data = new LinkedList<byte[]>();
  }
  
  
  @Override
  public Tuple readNext() {
    data.add(UUID.randomUUID().toString().getBytes());
    return new Tuple(Integer.toString(data.size()), 
                      data.getLast(),
                      new CommitLogEntry(this.getName(), data.size()-1, data.size()-1));
  }
  
  @Override
  public byte[] readFromOffset(long startOffset, long endOffset) {
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
