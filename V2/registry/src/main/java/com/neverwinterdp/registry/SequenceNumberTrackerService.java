package com.neverwinterdp.registry;

import com.google.inject.Inject;


public class SequenceNumberTrackerService {
  final static public String INTEGER_SEQUENCE_TRACKER_PATH = "/tracker/sequence/integer/";
  final static public byte[] EMPTY_DATA = new byte[0] ;

  @Inject
  private Registry registry ;
  
  public SequenceNumberTrackerService() { }
  
  public SequenceNumberTrackerService(Registry registry) {
    this.registry = registry;
  }
  
  public void createIntTrackerIfNotExist(String name) throws Exception {
    registry.createIfNotExist(INTEGER_SEQUENCE_TRACKER_PATH + name);
  }
  
  public int nextInt(String name) throws Exception {
    NodeInfo nodeInfo = registry.setData(INTEGER_SEQUENCE_TRACKER_PATH + name, EMPTY_DATA);
    return nodeInfo.getVersion();
  }
}
