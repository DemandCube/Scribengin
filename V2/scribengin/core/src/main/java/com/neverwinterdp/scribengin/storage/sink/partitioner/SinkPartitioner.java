package com.neverwinterdp.scribengin.storage.sink.partitioner;

public interface SinkPartitioner {
  String getPartition();

  String getPartition(long startOffset,  long endOffset);
}
