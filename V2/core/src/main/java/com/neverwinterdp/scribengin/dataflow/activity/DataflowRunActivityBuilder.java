package com.neverwinterdp.scribengin.dataflow.activity;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;

public class DataflowRunActivityBuilder extends AddWorkerActivityBuilder {
  public DataflowRunActivityBuilder( DataflowDescriptor dflDescriptor) {
    getActivity().setDescription("Run Dataflow Activity");
    getActivity().setType("run-dataflow");
    getActivity().withCoordinator(RunActivityCoordinator.class);
    for(int i = 0; i < dflDescriptor.getNumberOfWorkers(); i++) {
      add(new ActivityStep().
          withType("create-dataflow-worker").
          withExecutor(AddDataflowWorkerStepExecutor.class).
          attribute("worker.id", idTracker.getAndIncrement()));
    }
    add(new ActivityStep().
        withType("wait-for-worker-run-status").
        withExecutor(WaitForWorkerRunningStatus.class));
    
    add(new ActivityStep().
        withType("set-dataflow-run-status").
        withExecutor(SetRunningDataflowStatusStepExecutor.class));
  }
  
  @Singleton
  static public class RunActivityCoordinator extends ActivityCoordinator {
    @Inject
    DataflowActivityStepWorkerService activityStepWorkerService;
   
    @Override
    protected <T> void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) {
      activityStepWorkerService.exectute(activity, step);
    }
  }
  
  @Singleton
  static public class SetRunningDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      dflRegistry.setStatus(DataflowLifecycleStatus.RUNNING);
    }
  }
}
