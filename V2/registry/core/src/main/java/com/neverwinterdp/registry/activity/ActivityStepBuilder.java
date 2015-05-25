package com.neverwinterdp.registry.activity;

import java.util.List;

import com.google.inject.Injector;

public interface ActivityStepBuilder {
  public List<ActivityStep> build(Activity activity, Injector container) throws Exception ;
}
