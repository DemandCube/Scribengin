package com.neverwinterdp.scribengin.dataflow.service;

public interface DataflowServiceEventListener {
  static public enum Event { INIT }
  
  public void onEvent(DataflowService service, Event event) throws Exception ;
}