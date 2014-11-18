package com.neverwinterdp.scribengin.sink;

import com.neverwinterdp.scribengin.sink.partitioner.SinkPartitioner;
import com.neverwinterdp.scribengin.tuple.Tuple;

public interface SinkStream {
  void setSinkPartitioner(SinkPartitioner sp);
  boolean writeTuple(Tuple t);
  
  Tuple readFromOffset(long startOffset, long endOffset);
  
  //Used for recovery
  boolean removeFromOffset(long startOffset, long endOffset);
  boolean replaceAtOffset(Tuple t, long startOffset, long endOffset);
  
  boolean openStream();
  boolean closeStream();
  String getName();
  
  Tuple[] getCommittedData();
}
