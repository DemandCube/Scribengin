package com.neverwinterdp.scribengin.stream.sink.partitioner;

public interface SinkPartitioner {
  String getPartition();

  String getPartition(long startOffset,  long endOffset);
}
