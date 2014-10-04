package com.neverwinterdp.scribengin.partitioner;

import java.util.Date;

public abstract class AbstractPartitioner {
  public AbstractPartitioner(){}
  
  public abstract String getPartition();
  
  public abstract Date getRefresh();
}
