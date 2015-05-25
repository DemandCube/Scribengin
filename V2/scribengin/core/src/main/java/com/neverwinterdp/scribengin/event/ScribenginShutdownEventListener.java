package com.neverwinterdp.scribengin.event;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.NodeEventListener;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.scribengin.service.ScribenginService;

abstract public class ScribenginShutdownEventListener extends NodeEventListener<ScribenginEvent> {
  
  public ScribenginShutdownEventListener(Registry registry) throws RegistryException {
    super(registry, true);
    watch(ScribenginService.SHUTDOWN_EVENT_PATH);
  }

  @Override
  public ScribenginEvent toAppEvent(Registry registry, NodeEvent nodeEvent) throws Exception  {
    ScribenginEvent scribenginEvent = new ScribenginEvent("shutdown", nodeEvent) ;
    Boolean shutdown = registry.getDataAs(ScribenginService.SHUTDOWN_EVENT_PATH, Boolean.class);
    scribenginEvent.attr("shutdown", shutdown);
    return scribenginEvent;
  }

  @Override
  public void onEvent(ScribenginEvent event) throws Exception {
    boolean shutdown = event.attr("shutdown");
    if(shutdown) onShutdownEvent();
  }
  
  abstract public void onShutdownEvent() throws Exception ;
}