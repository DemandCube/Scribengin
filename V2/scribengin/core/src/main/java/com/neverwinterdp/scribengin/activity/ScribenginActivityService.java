package com.neverwinterdp.scribengin.activity;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.activity.ActivityService;
import com.neverwinterdp.scribengin.service.ScribenginService;

@Singleton
public class ScribenginActivityService extends ActivityService {
  
  @Inject
  public void onInit(Injector container) throws RegistryException {
    init(container, ScribenginService.ACTIVITIES_PATH);
  }
}
