package com.neverwinterdp.scribengin.dataflow.event;


import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.vm.event.VMWaitingEventListener;

public class DataflowWaitingEventListener extends VMWaitingEventListener {
  public DataflowWaitingEventListener(Registry registry) throws RegistryException {
    super(registry);
  }
  
  public void waitDataflowLeader(String desc, String dataflowName, String vmName) throws Exception {
    String dataflowLeaderPath = ScribenginService.getDataflowLeaderPath(dataflowName);
    waitingEventListeners.add(dataflowLeaderPath, new VMLeaderNodeEventMatcher(vmName), desc);
  }
  
  public void waitDataflowStatus(String desc, String dataflowName, DataflowLifecycleStatus status) throws Exception {
    String dataflowStatusPath = ScribenginService.getDataflowStatusPath(dataflowName);
    waitingEventListeners.add(dataflowStatusPath, status, desc);
  }
}