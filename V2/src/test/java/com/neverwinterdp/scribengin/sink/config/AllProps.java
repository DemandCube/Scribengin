package com.neverwinterdp.scribengin.sink.config;



import com.google.common.collect.Maps;

import java.util.Map;

public class AllProps {

  private Map<String, String> map = Maps.newHashMap();

  public AllProps() {
    map.put(Property.FOO.getName(), "ValueFromAllProps");
  }

  public String getString(Property p) {
    String value = map.get(p.getName());
    return value != null ? value : p.getDefaultValue();
  }

  @Override
  public String toString() {
    return map.toString();
  }
}
