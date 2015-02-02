package com.neverwinterdp.scribengin.event;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.event.AppEventListener;
import com.neverwinterdp.registry.event.NodeEvent;
import com.neverwinterdp.scribengin.service.ScribenginService;

abstract public class ScribenginShutdownEventListener extends AppEventListener<ScribenginEvent> {
  final static public String EVENT_PATH = ScribenginService.EVENTS_PATH + "/shutdown";
  
  public ScribenginShutdownEventListener(Registry registry) throws RegistryException {
    super(registry, true);
    watch(EVENT_PATH);
  }

  @Override
  public ScribenginEvent toAppEvent(Registry registry, NodeEvent nodeEvent) throws Exception  {
    ScribenginEvent scribenginEvent = new ScribenginEvent("shutdown", nodeEvent) ;
    Boolean shutdown = registry.getDataAs(EVENT_PATH, Boolean.class);
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
