package com.neverwinterdp.scribengin.sink;

import java.util.LinkedList;

import com.neverwinterdp.scribengin.scribe.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class InMemorySinkStream implements SinkStream{
  private LinkedList<Tuple> list;
  private SinkPartitioner sp;
  
  public InMemorySinkStream(SinkPartitioner sp){
    this.list = new LinkedList<Tuple>();
    this.sp = sp;
  }
  @Override
  public boolean writeTuple(Tuple t) {
    sp.getPartition();
    return list.add(t);
  }

  @Override
  public boolean openStream() {
    list.clear();
    return true;
  }

  @Override
  public boolean closeStream() {
    list.clear();
    return true;
  }
  
  @Override
  public void setSinkPartitioner(SinkPartitioner sp) {
    this.sp = sp;
  }

  public LinkedList<Tuple> getData(){
    return this.list;
  }
}
