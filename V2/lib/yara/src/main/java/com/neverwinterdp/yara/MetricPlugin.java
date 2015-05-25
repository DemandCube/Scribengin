package com.neverwinterdp.yara;

public interface MetricPlugin {
  public void onTimerUpdate(String name, long timestampTick, long duration) ;
  public void onCounterAdd(String name, long timestampTick, long incr) ;
}
