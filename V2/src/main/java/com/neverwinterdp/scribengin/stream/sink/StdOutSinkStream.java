package com.neverwinterdp.scribengin.stream.sink;

import java.util.LinkedList;
import java.util.UUID;

import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class StdOutSinkStream implements SinkStream{
  private LinkedList<Tuple> buffer;
  private String name;
  private long numTuples;
  
  public StdOutSinkStream(){
    buffer = new LinkedList<Tuple>();
    name = this.getClass().getSimpleName() +"-"+UUID.randomUUID().toString();
    numTuples = 0L;
  }
  
  public long getNumTuplesOutput(){
    return numTuples;
  }
  
  @Override
  public boolean prepareCommit() {
    return true;
  }

  @Override
  public boolean commit() {
    return true;
  }

  @Override
  public boolean clearBuffer() {
    buffer.clear();
    return true;
  }

  @Override
  public boolean completeCommit() {
    for(Tuple t: buffer){
      System.out.println(t.toString());
      numTuples++;
    }
    buffer.clear();
    return true;
  }

  @Override
  public boolean bufferTuple(Tuple t) {
    return buffer.add(t);
  }

  @Override
  public boolean rollBack() {
    buffer.clear();
    return true;
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

}
