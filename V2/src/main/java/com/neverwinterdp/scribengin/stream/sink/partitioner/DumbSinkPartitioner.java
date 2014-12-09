package com.neverwinterdp.scribengin.stream.sink.partitioner;

public class DumbSinkPartitioner implements SinkPartitioner{

  public DumbSinkPartitioner(){}
  
  @Override
  public String getPartition() {
    return "";
  }

  @Override
  public String getPartition(long startOffset, long endOffset) {
    // TODO Auto-generated method stub
    return null;
  }

}
