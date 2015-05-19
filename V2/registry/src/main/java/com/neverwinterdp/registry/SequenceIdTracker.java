package com.neverwinterdp.registry;

import java.text.DecimalFormat;



public class SequenceIdTracker {
  final static public byte[] EMPTY_DATA = new byte[0] ;
  static DecimalFormat SEQ_ID_FORMATER = new DecimalFormat("0000000000");

  private Registry registry ;
  private String   path;
  
  public SequenceIdTracker(Registry registry, String path) throws RegistryException {
    this.registry = registry;
    this.path = path ;
    registry.createIfNotExist(path);
  }
  
  public int nextInt() throws RegistryException {
    NodeInfo nodeInfo = registry.setData(path, EMPTY_DATA);
    return nodeInfo.getVersion();
  }
  
  public String nextSeqId() throws RegistryException {
    NodeInfo nodeInfo = registry.setData(path, EMPTY_DATA);
    int id = nodeInfo.getVersion();
    return SEQ_ID_FORMATER.format(id) ;
  }
}
