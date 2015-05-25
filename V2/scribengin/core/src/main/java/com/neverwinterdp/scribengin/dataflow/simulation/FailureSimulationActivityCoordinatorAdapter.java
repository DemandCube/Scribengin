package com.neverwinterdp.scribengin.dataflow.simulation;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityCoordinatorAdapter;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.scribengin.dataflow.simulation.FailureConfig.FailurePoint;

@Singleton
public class FailureSimulationActivityCoordinatorAdapter extends ActivityCoordinatorAdapter {
  @Inject
  private FailureSimulationService service;
  
  @Override
  public void beforeExecute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
    System.err.println("FailureSimulationActivityCoordinatorAdapter: before execute activity = " + activity.getId() + ", step = " + step.getId());
    service.runFailureSimulation(activity, step, FailurePoint.Before);
  }
  
  @Override
  public void afterExecute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
    System.err.println("FailureSimulationActivityCoordinatorAdapter: after execute activity = " + activity.getId() + ", step = " + step.getId());
    service.runFailureSimulation(activity, step, FailurePoint.After);
  }
}