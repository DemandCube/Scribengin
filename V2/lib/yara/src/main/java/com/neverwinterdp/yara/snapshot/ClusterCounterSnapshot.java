package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.yara.Counter;
import com.neverwinterdp.yara.cluster.ClusterCounter;

public class ClusterCounterSnapshot implements Serializable {
  private long count ;
  private Map<String, Long> counters = new HashMap<String, Long>() ;

  public ClusterCounterSnapshot() { }
  
  public ClusterCounterSnapshot(ClusterCounter clusterCounter) {
    this.count = clusterCounter.getCounter().getCount() ;
    for(Map.Entry<String, Counter> entry : clusterCounter.getCounters().entrySet()) {
      counters.put(entry.getKey(), entry.getValue().getCount()) ;
    }
  }

  public long getCount() { return count; }
  public void setCount(long count) { this.count = count; }

  public Map<String, Long> getCounters() { return counters; }
  public void setCounters(Map<String, Long> counters) { this.counters = counters;}
}
