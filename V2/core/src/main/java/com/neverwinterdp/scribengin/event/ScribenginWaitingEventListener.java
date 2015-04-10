package com.neverwinterdp.scribengin.event;


import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.vm.event.VMWaitingEventListener;

public class ScribenginWaitingEventListener extends VMWaitingEventListener {
  public ScribenginWaitingEventListener(Registry registry) throws RegistryException {
    super(registry);
  }
  
  public void waitScribenginMaster(String desc, String vmName) throws Exception {
    waitingEventListeners.add(ScribenginService.LEADER_PATH, new VMLeaderNodeEventMatcher(vmName), desc);
  }
}