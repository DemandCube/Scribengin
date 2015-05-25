package com.neverwinterdp.yara.quantile.util;

import java.util.HashMap;

///TODO: implement a native long hash map for better performance
public class LongHashMap extends HashMap<Long, Long> {
  
  public long get(long key) {
    Long val = super.get(key) ;
    if(val != null) return val ;
    return 0 ;
  }
  
  public void addTo(long key, long incr) {
    Long val = get(key) ;
    if(val != null) {
      put(key, val.longValue() + incr) ;
    } else {
      put(key, incr) ;
    }
  }
}
