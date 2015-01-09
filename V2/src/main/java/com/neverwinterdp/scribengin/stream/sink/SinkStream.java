package com.neverwinterdp.scribengin.stream.sink;

import com.neverwinterdp.scribengin.stream.Stream;
import com.neverwinterdp.scribengin.stream.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public interface SinkStream extends Stream{
  
  public boolean bufferTuple(Tuple t);
  void setSinkPartitioner(SinkPartitioner sp);
  String getName();
  
  public long getBufferSize();
}
