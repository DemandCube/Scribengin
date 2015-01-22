package com.neverwinterdp.registry.event;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.neverwinterdp.registry.Registry;

public class SequenceEventListener extends EventListener {
  private List<EventListener> eventListeners = new ArrayList<EventListener>();
  protected Registry registry;
  protected RegistryListener registryListener ;
  
  public SequenceEventListener(String desc, Registry registry) {
    super(desc) ;
    this.registry = registry;
    registryListener = new RegistryListener(registry);
  }
  
  public void add(EventListener unit) {
    eventListeners.add(unit);
  }
  
  synchronized public boolean process(Event event) {
    boolean process = false;
    Iterator<EventListener> i = eventListeners.iterator();
    while(i.hasNext()) {
      EventListener unit = i.next();
      if(unit.process(event)) {
        i.remove();
        process = true ;
        break;
      }
    }
    notifyAll();
    return process ;
  }
  
  synchronized public void waitForEvents(long timeout) throws Exception {
    long stopTime = System.currentTimeMillis() + timeout;
    try {
      while(true) {
        long waitTime = stopTime - System.currentTimeMillis();
        if(waitTime <= 0) return;
        wait(waitTime);
        if(eventListeners.size() == 0) return ;
      }
    } catch (InterruptedException e) {
      throw new Exception("Cannot wait for the events in " + timeout + "ms") ;
    } finally {
      if(eventListeners.size() > 0) {
        for(EventListener sel : eventListeners) {
          System.err.println("Expect: " + sel.getDescription());
        }
        throw new Exception("Cannot wait for the events in " + timeout + "ms") ;
      }
    }
  }
  
  public void reset() { eventListeners.clear(); }
}
