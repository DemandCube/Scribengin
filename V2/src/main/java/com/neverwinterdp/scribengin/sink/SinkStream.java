package com.neverwinterdp.scribengin.sink;

import com.neverwinterdp.scribengin.scribe.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public interface SinkStream {
  void setSinkPartitioner(SinkPartitioner sp);
  boolean writeTuple(Tuple t);
  boolean openStream();
  boolean closeStream();

}