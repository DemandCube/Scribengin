package com.neverwinterdp.yara;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class Counter implements Serializable {
  transient private MetricPlugin metricPlugin ;
  private String name ;
  private AtomicLong counter = new AtomicLong() ;

  public Counter() {} 
  
  public Counter(String name) {
    this.name = name ;
  }
  
  public String getName() { return this.name ; }
  
  public long getCount() { return counter.longValue() ; }
  
  public void setMetricPlugin(MetricPlugin plugin) {
    this.metricPlugin = plugin ;
  }
  
  public long incr() { return incr(1l);  }
  
  public long incr(long n) { return add(Clock.defaultClock().getTick(), n) ; }
  
  public long decr() { 
    return add(Clock.defaultClock().getTick(), -1l); 
  }
  
  public long decr(long count) { 
    return add(Clock.defaultClock().getTick(), -count); 
  }
  
  public long add(long timestampTick, long n) {
    if(metricPlugin != null) {
      metricPlugin.onCounterAdd(name, timestampTick, n);
    }
    return counter.addAndGet(n);
  }
  
  static public Counter combine(Counter ... counters) {
    Counter counter = new Counter() ;
    for(Counter sel : counters) {
      counter.incr(sel.getCount()) ;
    }
    return counter ;
  }
  
  static public Counter combine(Collection<Counter>  counters) {
    Counter[] array = new Counter[counters.size()] ;
    counters.toArray(array) ;
    return combine(array) ;
  }
}
