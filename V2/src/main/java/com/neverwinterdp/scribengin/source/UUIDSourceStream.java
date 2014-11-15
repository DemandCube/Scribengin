package com.neverwinterdp.scribengin.source;

import java.util.UUID;

import com.neverwinterdp.scribengin.tuple.Tuple;

public class UUIDSourceStream implements SourceStream{

  private Integer key;
  
  public UUIDSourceStream(){
    key = 0;
  }
  
  @Override
  public Tuple readNext() {
    return new Tuple((key++).toString(), UUID.randomUUID().toString().getBytes());
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
    return key;
  }
}
