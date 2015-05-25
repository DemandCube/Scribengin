package com.neverwinterdp.yara.cluster;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.neverwinterdp.yara.Counter;

public class ClusterCounter {
  private boolean modified = false ;
  private Counter counter = new Counter() ;
  private Map<String, Counter> counters = new ConcurrentHashMap<String, Counter>() ;
  
  public Counter getCounter() {
    updateIfModified() ;
    return this.counter ; 
  }
  
  public void update(String name, Counter counter) {
    modified = true ;
    counters.put(name, counter) ;
  }
 
  public void update(String name, long timestampTick, long add) {
    modified = true ;
    Counter counter = getCounter(name, true) ;
    counter.add(timestampTick, add) ;
  }
 
  
  public Map<String, Counter> getCounters() { return this.counters ; }

  private Counter getCounter(String name, boolean create) {
    Counter counter = counters.get(name) ;
    if(counter != null) return counter ;
    if(!create) return null ;
    synchronized(counters) {
      counter = counters.get(name) ;
      if(counter != null) return counter ;
      counter = new Counter(name) ;
      counters.put(name, counter) ;
    }
    return counter ;
  }
  
  private void updateIfModified() {
    if(!modified) return ;
    counter = Counter.combine(counters.values()) ;
    modified = false ;
  }
}
