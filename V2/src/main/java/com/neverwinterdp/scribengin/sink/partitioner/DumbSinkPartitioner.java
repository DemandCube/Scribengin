package com.neverwinterdp.scribengin.sink.partitioner;

public class DumbSinkPartitioner implements SinkPartitioner{

  public DumbSinkPartitioner(){}
  
  @Override
  public String getPartition() {
    return "";
  }

}
