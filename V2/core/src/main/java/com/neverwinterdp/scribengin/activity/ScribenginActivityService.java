package com.neverwinterdp.scribengin.activity;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.ActivityService;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;

@Singleton
public class ScribenginActivityService extends ActivityService {
  
  @Inject
  public void onInit(Injector container, DataflowRegistry dataflowRegistry) throws RegistryException {
    init(container, dataflowRegistry.getActivitiesPath());
  }
}
