package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.yara.cluster.ClusterCounter;
import com.neverwinterdp.yara.cluster.ClusterMetricRegistry;
import com.neverwinterdp.yara.cluster.ClusterTimer;

public class ClusterMetricRegistrySnapshot implements Serializable {
  private Map<String, ClusterCounterSnapshot> counters = new TreeMap<String, ClusterCounterSnapshot>();
  private Map<String, ClusterTimerSnapshot> timers = new TreeMap<String, ClusterTimerSnapshot>();

  public ClusterMetricRegistrySnapshot() { }
  
  public ClusterMetricRegistrySnapshot(ClusterMetricRegistry registry) {
    this(registry, TimeUnit.MILLISECONDS) ;
  }
  
  public ClusterMetricRegistrySnapshot(ClusterMetricRegistry registry, TimeUnit timeUnit) {
    if(registry == null) return  ;
    for(Map.Entry<String, ClusterCounter> entry : registry.getCounters().entrySet()) {
      counters.put(entry.getKey(), new ClusterCounterSnapshot(entry.getValue())) ;
    }
    
    for(Map.Entry<String, ClusterTimer> entry : registry.getTimers().entrySet()) {
      timers.put(entry.getKey(), new ClusterTimerSnapshot(entry.getValue(), timeUnit)) ;
    }
  }

  public Map<String, ClusterCounterSnapshot> getCounters() { return counters; }

  public void setCounters(Map<String, ClusterCounterSnapshot> counters) { this.counters = counters; }

  public Map<String, ClusterTimerSnapshot> getTimers() { return timers; }

  public void setTimers(Map<String, ClusterTimerSnapshot> timers) { this.timers = timers; }
}
