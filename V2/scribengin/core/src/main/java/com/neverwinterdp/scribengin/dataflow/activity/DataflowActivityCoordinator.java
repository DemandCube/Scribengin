package com.neverwinterdp.scribengin.dataflow.activity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.scribengin.dataflow.simulation.FailureSimulationActivityCoordinatorAdapter;

@Singleton
public class DataflowActivityCoordinator extends ActivityCoordinator {
  @Inject
  DataflowActivityStepWorkerService activityStepWorkerService;

  @Inject
  public void init(FailureSimulationActivityCoordinatorAdapter failureSimulationAdapter) {
    add(failureSimulationAdapter);
  }
  
  @Override
  protected <T> void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
    activityStepWorkerService.exectute(context, activity, step);
  }
}