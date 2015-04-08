package com.neverwinterdp.scribengin.event;


import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.vm.event.VMWaitingEventListenerNew;

public class ScribenginWaitingEventListenerNew extends VMWaitingEventListenerNew {
  public ScribenginWaitingEventListenerNew(Registry registry) throws RegistryException {
    super(registry);
  }
  
  public void waitScribenginMaster(String desc, String vmName) throws Exception {
    waitingEventListeners.add(ScribenginService.LEADER_PATH, new VMLeaderNodeEventMatcher(vmName));
  }
  
  public void waitDataflowLeader(String desc, String dataflowName, String vmName) throws Exception {
    String dataflowLeaderPath = ScribenginService.getDataflowLeaderPath(dataflowName);
    waitingEventListeners.add(dataflowLeaderPath, new VMLeaderNodeEventMatcher(vmName));
  }
  
  public void waitDataflowStatus(String desc, String dataflowName, DataflowLifecycleStatus status) throws Exception {
    String dataflowStatusPath = ScribenginService.getDataflowStatusPath(dataflowName);
    waitingEventListeners.add(dataflowStatusPath, status);
  }
}