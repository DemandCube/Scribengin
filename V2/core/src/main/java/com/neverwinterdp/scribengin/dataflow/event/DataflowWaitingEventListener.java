package com.neverwinterdp.scribengin.dataflow.event;


import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.vm.event.VMWaitingEventListener;

public class DataflowWaitingEventListener extends VMWaitingEventListener {
  public DataflowWaitingEventListener(Registry registry) throws RegistryException {
    super(registry);
  }
  
  public void waitDataflowLeader(String desc, DataflowDescriptor descriptor, String vmName) throws Exception {
    String dataflowLeaderPath = ScribenginService.getDataflowLeaderPath(descriptor.getId());
    waitingEventListeners.add(dataflowLeaderPath, new VMLeaderNodeEventMatcher(vmName), desc);
  }
  
  public void waitDataflowStatus(String desc, DataflowDescriptor descriptor, DataflowLifecycleStatus status) throws Exception {
    String dataflowStatusPath = ScribenginService.getDataflowStatusPath(descriptor.getId());
    waitingEventListeners.add(dataflowStatusPath, status, desc);
  }
}