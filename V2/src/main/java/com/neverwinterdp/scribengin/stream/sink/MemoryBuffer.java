package com.neverwinterdp.scribengin.stream.sink;

import java.io.File;
import java.util.LinkedList;

import com.neverwinterdp.scribengin.stream.sink.Buffer.State;
import com.neverwinterdp.scribengin.tuple.Tuple;

public class MemoryBuffer extends Buffer {
  private int tuplesSize;
  /** The buffer. */
  private LinkedList<Tuple> tuples = new LinkedList<Tuple>();
  
  
  public MemoryBuffer(S3SinkConfig config) {
    super(config.getMemoryMaxBufferSize(), config.getMemoryMaxBufferingTime(), config.getMemoryMaxTuples());
  }

  @Override
  public void add(Tuple tuple) {
    state = State.Appending;
    tuples.add(tuple);
    updateState();
    tuplesSize += tuple.getData().length;
  }

  @Override
  public int getTuplesCount() { 
    return tuples.size();
  }

  @Override
  public int getTuplesSize() {
    return tuplesSize;
  }

  @Override
  public void clean() {
    tuples.clear();
  }
  public Tuple poll() {
    state = State.Purging;
    return tuples.poll();
  }
}
