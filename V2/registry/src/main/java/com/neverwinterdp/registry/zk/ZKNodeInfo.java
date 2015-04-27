package com.neverwinterdp.registry.zk;

import org.apache.zookeeper.data.Stat;

import com.neverwinterdp.registry.NodeInfo;

public class ZKNodeInfo implements NodeInfo {
  private Stat stat ;
  
  public ZKNodeInfo(Stat stat) {
    this.stat = stat ;
  }
  
  @Override
  public int getVersion() { return stat.getVersion(); }
}
