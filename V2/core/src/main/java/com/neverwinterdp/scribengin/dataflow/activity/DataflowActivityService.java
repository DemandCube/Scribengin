package com.neverwinterdp.scribengin.dataflow.activity;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.ActivityService;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;

@Singleton
public class DataflowActivityService extends ActivityService {
  
  @Inject
  public void onInit(Injector container, DataflowRegistry dataflowRegistry) throws RegistryException {
    init(container, dataflowRegistry.getActivitiesPath());
  }
}
