package com.neverwinterdp.scribengin.junit;

import com.neverwinterdp.registry.junit.AssertEvent;
import com.neverwinterdp.registry.junit.AssertUnit;

abstract public class ScribenginAssertUnit extends AssertUnit {
  protected ScribenginAssertUnit(String description) {
    super(description);
  }

  @Override
  public boolean assertEvent(AssertEvent event) {
    if(!(event instanceof ScribenginAssertEvent)) return false;
    return assertEvent((ScribenginAssertEvent) event);
  }

  abstract public boolean assertEvent(ScribenginAssertEvent event) ;
}
