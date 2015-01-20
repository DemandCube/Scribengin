package com.neverwinterdp.vm.junit;

import com.neverwinterdp.registry.junit.AssertEvent;
import com.neverwinterdp.registry.junit.AssertUnit;

abstract public class VMAssertUnit extends AssertUnit {
  protected VMAssertUnit(String description) {
    super(description);
  }

  @Override
  public boolean assertEvent(AssertEvent event) {
    if(!(event instanceof VMAssertEvent)) return false;
    VMAssertEvent vmAssertEvent = (VMAssertEvent) event;
    return assertEvent(vmAssertEvent);
  }

  abstract public boolean assertEvent(VMAssertEvent event) ;
}
