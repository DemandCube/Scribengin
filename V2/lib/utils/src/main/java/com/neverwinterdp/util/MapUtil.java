package com.neverwinterdp.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.neverwinterdp.util.text.StringUtil;

public class MapUtil {
  static public boolean getBool(Map<String, String> map, String name, boolean defaultValue) {
    if(map == null) return defaultValue ;
    String val = map.get(name) ;
    if(val != null) return Boolean.parseBoolean(val) ;
    return defaultValue ;
  }
  
  static public int getInteger(Map<String, String> map, String name, int defaultValue) {
    if(map == null) return defaultValue ;
    String val = map.get(name) ;
    if(val != null) return Integer.parseInt(val) ;
    return defaultValue ;
  }
  
  static public long getLong(Map<String, String> map, String name, long defaultValue) {
    if(map == null) return defaultValue ;
    String val = map.get(name) ;
    if(val != null) return Long.parseLong(val) ;
    return defaultValue ;
  }
  
  static public String getString(Map<String, String> map, String name, String defaultValue) {
    if(map == null) return defaultValue ;
    String val = map.get(name) ;
    if(val != null) return val ;
    return defaultValue ;
  }
  
  static public String[] getStringArray(Map<String, String> map, String name, String[] defaultValue) {
    if(map == null) return defaultValue ;
    String val = map.get(name) ;
    if(val != null) return StringUtil.toStringArray(val) ;
    return defaultValue ;
  }
  
  static public Map<String, String> getSubMap(Map<String, String> map, String prefix) {
    if(map == null) return null ;
    Map<String, String> submap = new HashMap<String, String>() ;
    Iterator<Map.Entry<String, String>> i = map.entrySet().iterator() ;
    while(i.hasNext()) {
      Map.Entry<String, String> entry = i.next() ;
      String key = entry.getKey() ;
      if(key.startsWith(prefix)) {
        key = key.substring(prefix.length()) ;
        submap.put(key, entry.getValue()) ;
      }
    }
    return submap ;
  }
}
