package com.neverwinterdp.scribengin.event;

import com.neverwinterdp.registry.event.Event;
import com.neverwinterdp.registry.event.EventListener;

abstract public class ScribenginEventListener extends EventListener {
  protected ScribenginEventListener(String description) {
    super(description);
  }

  @Override
  public boolean process(Event event) {
    if(!(event instanceof ScribenginEvent)) return false;
    return process((ScribenginEvent) event);
  }

  abstract public boolean process(ScribenginEvent event) ;
}
