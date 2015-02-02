package com.neverwinterdp.vm.event;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.AppEventListener;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.vm.service.VMService;

abstract public class VMShutdownEventListener extends AppEventListener<VMEvent> {
  final static public String EVENT_PATH = VMService.EVENTS_PATH + "/shutdown";
  
  public VMShutdownEventListener(Registry registry) throws RegistryException {
    super(registry, true);
    watch(EVENT_PATH);
  }

  @Override
  public VMEvent toAppEvent(Registry registry, NodeEvent nodeEvent) throws Exception  {
    VMEvent scribenginEvent = new VMEvent("shutdown", nodeEvent) ;
    Boolean shutdown = registry.getDataAs(EVENT_PATH, Boolean.class);
    scribenginEvent.attr("shutdown", shutdown);
    return scribenginEvent;
  }

  @Override
  public void onEvent(VMEvent event) throws Exception {
    boolean shutdown = event.attr("shutdown");
    if(shutdown) onShutdownEvent();
  }
  
  abstract public void onShutdownEvent() throws Exception ;
}