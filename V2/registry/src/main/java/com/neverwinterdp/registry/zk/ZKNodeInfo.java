package com.neverwinterdp.registry.zk;

import org.apache.zookeeper.data.Stat;

import com.neverwinterdp.registry.NodeInfo;

public class ZKNodeInfo implements NodeInfo {
  private Stat stat ;
  
  public ZKNodeInfo(Stat stat) {
    this.stat = stat ;
  }
  
  @Override
  public long getCtime() { return stat.getCtime() ; }
 
  @Override
  public long getMtime() { return stat.getMtime() ; }
  
  @Override
  public int getVersion() { return stat.getVersion(); }

  @Override
  public int getDataLength() { return stat.getDataLength(); }
  
  @Override
  public int getNumOfChildren() { return stat.getNumChildren() ; }
}
