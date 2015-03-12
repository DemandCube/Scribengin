package com.neverwinterdp.scribengin.sink.partitioner;

public interface SinkPartitioner {
  String getPartition();

  String getPartition(long startOffset,  long endOffset);
}
