package com.neverwinterdp.scribengin.dataflow.master;

public interface EventListener {
  static public enum Event { INIT }
  
  public void onEvent(DataflowMaster master, Event event) throws Exception ;
}
