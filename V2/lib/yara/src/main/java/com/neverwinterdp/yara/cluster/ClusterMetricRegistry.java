package com.neverwinterdp.yara.cluster;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.neverwinterdp.yara.Counter;
import com.neverwinterdp.yara.MetricRegistry;
import com.neverwinterdp.yara.Timer;

public class ClusterMetricRegistry {
  private Map<String, ClusterCounter> clusterCounters = new ConcurrentHashMap<String, ClusterCounter>() ;
  private Map<String, ClusterTimer>   clusterTimers   = new ConcurrentHashMap<String, ClusterTimer>() ;

  public ClusterCounter getCounter(String name) { return clusterCounters.get(name) ; }
  
  public ClusterTimer getTimer(String name) { return clusterTimers.get(name) ; }
  
  public Map<String, ClusterCounter> getCounters() { return this.clusterCounters ; }
  
  public Map<String, ClusterTimer> getTimers() { return clusterTimers ; }
  
  synchronized public void update(MetricRegistry registry) {
    Map<String, Counter> counters = registry.getCounters() ;
    Iterator<Map.Entry<String, Counter>> counterItr = counters.entrySet().iterator() ;
    while(counterItr.hasNext()) {
      Map.Entry<String, Counter> entry = counterItr.next() ;
      String key = entry.getKey() ;
      ClusterCounter clusterCounter = clusterCounters.get(key) ;
      if(clusterCounter == null) {
        clusterCounter = new ClusterCounter() ;
        clusterCounters.put(key, clusterCounter) ;
      }
      clusterCounter.update(registry.getName(), entry.getValue());
    }
    
    for(Map.Entry<String, Timer> entry : registry.getTimers().entrySet()) {
      String key = entry.getKey() ;
      ClusterTimer clusterTimer = clusterTimers.get(key) ;
      if(clusterTimer == null) {
        clusterTimer = new ClusterTimer() ;
        clusterTimers.put(key, clusterTimer) ;
      }
      clusterTimer.update(registry.getName(), entry.getValue());
    }
  }
  
  public ClusterCounter counter(String name) {
    ClusterCounter clusterCounter = clusterCounters.get(name) ;
    if(clusterCounter != null) return clusterCounter ;
    synchronized(clusterCounters) {
      clusterCounter = clusterCounters.get(name) ;
      if(clusterCounter != null) return clusterCounter ;
      clusterCounter = new ClusterCounter() ;
      clusterCounters.put(name, clusterCounter) ;
    }
    return clusterCounter ;
  }
  
  public ClusterTimer timer(String name) {
    ClusterTimer clusterTimer = clusterTimers.get(name) ;
    if(clusterTimer != null) return clusterTimer ;
    synchronized(clusterTimers) {
      clusterTimer = clusterTimers.get(name) ;
      if(clusterTimer != null) return clusterTimer ;
      clusterTimer = new ClusterTimer() ;
      clusterTimers.put(name, clusterTimer) ;
    }
    return clusterTimer ;
  }
}