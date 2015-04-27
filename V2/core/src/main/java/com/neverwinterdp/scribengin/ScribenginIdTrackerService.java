package com.neverwinterdp.scribengin;

import javax.annotation.PostConstruct;

import com.google.inject.Inject;
import com.neverwinterdp.registry.SequenceNumberTrackerService;

public class ScribenginIdTrackerService {
  final static public String DATAFLOW_WORKER_ID_TRACKER = "scribengin-dataflow-worker";
  final static public String DATAFLOW_MASTER_ID_TRACKER = "scribengin-dataflow-master";
  
  @Inject
  private SequenceNumberTrackerService idTrackerService ;

  @PostConstruct
  public void onInit() throws Exception {
    idTrackerService.createIntTrackerIfNotExist(DATAFLOW_MASTER_ID_TRACKER); 
    idTrackerService.createIntTrackerIfNotExist(DATAFLOW_WORKER_ID_TRACKER); 
  }

  public int nextDataflowMasterId() throws Exception {
    return idTrackerService.nextInt(DATAFLOW_MASTER_ID_TRACKER);
  }
  
  public int nextDataflowWorkerId() throws Exception {
    return idTrackerService.nextInt(DATAFLOW_WORKER_ID_TRACKER);
  }
  
}
