package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.SequenceNumberTrackerService;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.scribengin.ScribenginIdTrackerService;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;

public class DataflowRunActivityBuilder extends AddWorkerActivityBuilder {
  public Activity build() {
    Activity activity = new Activity() ;
    activity.setDescription("Run Dataflow Activity");
    activity.setType("run-dataflow");
    activity.withCoordinator(RunActivityCoordinator.class);
    activity.withActivityStepBuilder(DataflowRunActivityStepBuilder.class);
    return activity;
  }
  
  @Singleton
  static public class DataflowRunActivityStepBuilder implements ActivityStepBuilder {
    @Inject
    private DataflowService service ;
    
    @Inject
    private ScribenginIdTrackerService idTrackerService ;
    
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      DataflowDescriptor dflDescriptor = service.getDataflowRegistry().getDataflowDescriptor();
      List<ActivityStep> steps = new ArrayList<>() ;
      for(int i = 0; i < dflDescriptor.getNumberOfWorkers(); i++) {
        ActivityStep addWorkerStep = AddDataflowWorkerActivityStepBuilder.createAddDataflowWorkerStep(idTrackerService);
        steps.add(addWorkerStep);
      }
      steps.add(new ActivityStep().
          withType("wait-for-worker-run-status").
          withExecutor(WaitForWorkerRunningStatus.class));
      
      steps.add(new ActivityStep().
          withType("set-dataflow-run-status").
          withExecutor(SetRunningDataflowStatusStepExecutor.class));
      return steps;
    }
  }
  
  @Singleton
  static public class RunActivityCoordinator extends ActivityCoordinator {
    @Inject
    DataflowActivityStepWorkerService activityStepWorkerService;
   
    @Override
    protected <T> void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
      activityStepWorkerService.exectute(context, activity, step);
    }
  }
  
  @Singleton
  static public class SetRunningDataflowStatusStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      dflRegistry.setStatus(DataflowLifecycleStatus.RUNNING);
    }
  }
}
