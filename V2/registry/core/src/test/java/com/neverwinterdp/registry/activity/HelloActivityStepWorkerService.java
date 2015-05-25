package com.neverwinterdp.registry.activity;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;

@Singleton
public class HelloActivityStepWorkerService extends ActivityStepWorkerService<String> {
  @Inject
  public HelloActivityStepWorkerService(Injector container) throws RegistryException {
    super("HelloWorker");
  }
}
