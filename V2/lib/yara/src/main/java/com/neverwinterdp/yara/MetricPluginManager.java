package com.neverwinterdp.yara;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MetricPluginManager implements MetricPlugin {
  private List<MetricPlugin> plugins = new ArrayList<MetricPlugin>() ;

  public int size() { return plugins.size() ; }
  
  public void add(MetricPlugin plugin) {
    plugins.add(plugin) ;
  }
  
  public <T extends MetricPlugin> T remove(Class<T> type) {
    Iterator<MetricPlugin> i = plugins.iterator() ;
    while(i.hasNext()) {
      MetricPlugin plugin = i.next() ;
      if(type.isInstance(plugin)) {
        i.remove(); 
        return (T)plugin ;
      }
    }
    return null ;
  }
  
  public void onTimerUpdate(String name, long timestampTick, long duration) {
    for(int i = 0; i < plugins.size(); i++) {
      MetricPlugin plugin = plugins.get(i) ;
      plugin.onTimerUpdate(name, timestampTick, duration);
    }
  }
  
  public void onCounterAdd(String name, long timestampTick, long incr) {
    for(int i = 0; i < plugins.size(); i++) {
      MetricPlugin plugin = plugins.get(i) ;
      plugin.onCounterAdd(name, timestampTick, incr);
    }
  }
}
