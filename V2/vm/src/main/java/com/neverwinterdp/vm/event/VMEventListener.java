package com.neverwinterdp.vm.event;

import com.neverwinterdp.registry.event.Event;
import com.neverwinterdp.registry.event.EventListener;

abstract public class VMEventListener extends EventListener {
  protected VMEventListener(String description) {
    super(description);
  }

  @Override
  public boolean process(Event event) {
    if(!(event instanceof VMEvent)) return false;
    VMEvent vmAssertEvent = (VMEvent) event;
    return process(vmAssertEvent);
  }

  abstract public boolean process(VMEvent event) ;
}
