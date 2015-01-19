package com.neverwinterdp.registry.junit;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RegistryAssert {
  private List<AssertUnit> assertUnits = new ArrayList<AssertUnit>();
  
  public void add(AssertUnit unit) {
    assertUnits.add(unit);
  }
  
  synchronized public void assertEvent(AssertEvent event) {
    Iterator<AssertUnit> i = assertUnits.iterator();
    while(i.hasNext()) {
      AssertUnit unit = i.next();
      if(unit.assertEvent(event)) {
        i.remove();
        break;
      }
    }
    notifyAll();
  }
  
  synchronized public void waitForEvents(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    try {
      while(true) {
        long waitTime = stopTime - System.currentTimeMillis();
        if(waitTime <= 0) return;
        wait(waitTime);
        if(assertUnits.size() == 0) return ;
      }
    } catch (InterruptedException e) {
      throw new Exception("Cannot wait for the events in " + timeout + "ms") ;
    } finally {
      if(assertUnits.size() > 0) {
        for(AssertUnit sel : assertUnits) {
          System.err.println("Expect: " + sel.getDescription());
        }
        throw new Exception("Cannot wait for the events in " + timeout + "ms") ;
      }
    }
  }
  
  public void reset() { assertUnits.clear(); }
}
