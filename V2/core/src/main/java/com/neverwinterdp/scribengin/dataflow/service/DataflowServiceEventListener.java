package com.neverwinterdp.scribengin.dataflow.service;

import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;

public interface DataflowServiceEventListener {
  public void onEvent(DataflowService service, DataflowLifecycleStatus event) throws Exception ;
}