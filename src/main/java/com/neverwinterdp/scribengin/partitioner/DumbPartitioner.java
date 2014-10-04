package com.neverwinterdp.scribengin.partitioner;

import java.util.Date;

public class DumbPartitioner extends AbstractPartitioner{
  public DumbPartitioner(){}
  
  @Override
  public String getPartition() {
    return "";
  }

  @Override
  public Date getRefresh() {
    return null;
  }

}
