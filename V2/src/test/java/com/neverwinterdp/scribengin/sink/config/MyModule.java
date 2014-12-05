package com.neverwinterdp.scribengin.sink.config;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;

public class MyModule implements Module {
  @Override
  public void configure(Binder binder) {
    AllProps allProps = new AllProps();
    for (Property prop : Property.values()) {
      String value = getValue(allProps, prop);
      binder.bind(Key.get(prop.getValueType(), new PropImpl(prop)))
        .toInstance(value);
    }
    binder.bind(AllProps.class).toInstance(allProps);
  }

  private String getValue(AllProps allProps, Property p) {
    String v = allProps.getString(p);
    return v == null ? p.getDefaultValue() : v;
  }
}
