package com.neverwinterdp.yara.snapshot;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.neverwinterdp.yara.Timer;
import com.neverwinterdp.yara.cluster.ClusterTimer;

public class ClusterTimerSnapshot implements Serializable {
  private TimerSnapshot timer ;
  private Map<String, TimerSnapshot> timers = new HashMap<String, TimerSnapshot>() ;

  public ClusterTimerSnapshot() { }
  
  public ClusterTimerSnapshot(ClusterTimer clusterTimer, TimeUnit timeUnit) {
    timer = new TimerSnapshot(clusterTimer.getTimer(), timeUnit) ;
    for(Map.Entry<String, Timer> entry : clusterTimer.getTimers().entrySet()) {
      timers.put(entry.getKey(), new TimerSnapshot(entry.getValue(), timeUnit)) ;
    }
  }

  public TimerSnapshot getTimer() { return timer; }

  public void setTimer(TimerSnapshot timer) { this.timer = timer; }

  public Map<String, TimerSnapshot> getTimers() { return timers; }

  public void setTimers(Map<String, TimerSnapshot> timers) { this.timers = timers; }
}
