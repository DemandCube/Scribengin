package com.neverwinterdp.registry;



public class SequenceIdTracker {
  final static public byte[] EMPTY_DATA = new byte[0] ;

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
}
