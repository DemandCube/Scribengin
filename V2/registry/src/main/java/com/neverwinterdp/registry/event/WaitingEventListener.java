package com.neverwinterdp.registry.event;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.neverwinterdp.registry.DataChangeNodeWatcher;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;

public class WaitingEventListener extends EventListener {
  protected List<EventListener> eventListeners = new ArrayList<EventListener>();
  protected Registry registry;
  protected RegistryListener registryListener ;
  
  public WaitingEventListener(String desc, Registry registry) {
    super(desc) ;
    this.registry = registry;
    registryListener = new RegistryListener(registry);
  }
  
  public void add(EventListener unit) {
    eventListeners.add(unit);
  }

  public <T> void add(String desc, String path, boolean persistent, final Class<T> type, final T expectData) throws RegistryException {
    DataChangeNodeWatcher<T> watcher = new DataChangeNodeWatcher<T>(registry, type) {
      @Override
      public void onChange(NodeEvent nodeEvent, T data) {
        Event event = new Event("waiting-for-data", nodeEvent) ;
        event.attr(type, data);
        WaitingEventListener.this.process(event);
      }
    };
    EventListener unitListener = new EventListener(desc) {
      @Override
      public boolean process(Event event) {
        if(!"waiting-for-data".equals(event.getName())) return false;
        T data = event.attr(type);
        return data.equals(expectData);
      }
    };
    registryListener.watch(path, watcher, persistent);
    eventListeners.add(unitListener);
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
    if(eventListeners.size() == 0) return ;
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
