package com.neverwinterdp.scribengin.activity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowLifecycleStatus;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.service.ScribenginService;

public class ShutdownDataflowMasterActivityBuilder extends ActivityBuilder {
  static public AtomicInteger idTracker = new AtomicInteger(1) ;
  
  public Activity build(String dataflowPath) {
    Activity activity = new Activity();
    activity.setDescription("Shutdown Dataflow Master Activity");
    activity.setType("shutdown-dataflow-master");
    activity.attribute("dataflow.path", dataflowPath);
    activity.withCoordinator(ShutdownDataflowMasterActivityCoordinator.class);
    activity.withActivityStepBuilder(ShutdownDataflowMasterActivityStepBuilder.class) ;
    return activity;
  }
  
  @Singleton
  static public class ShutdownDataflowMasterActivityStepBuilder implements ActivityStepBuilder {
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      steps.add(new ActivityStep().
          withType("shutdown-dataflow-master").
          withExecutor(ShutdownDataflowMasterStepExecutor.class));
      return steps;
    }
  }
  
  @Singleton
  static public class ShutdownDataflowMasterActivityCoordinator extends ActivityCoordinator {
    @Inject
    ScribenginActivityStepWorkerService activityStepWorkerService;
   
    @Override
    protected <T> void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
      activityStepWorkerService.exectute(context, activity, step);
    }
  }
  
  @Singleton
  static public class ShutdownDataflowMasterStepExecutor implements ActivityStepExecutor {
    @Inject
    private Registry registry ;
    
    @Inject
    private ScribenginService scribenginService;
    
    @Override
    public void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
      System.err.println("ShutdownDataflowMasterStepExecutor: execute().............");
      String dataflowPath = activity.attribute("dataflow.path");
      DataflowRegistry dataflowRegistry = new DataflowRegistry(registry, dataflowPath) ;
      while(dataflowRegistry.countDataflowMasters() > 0) {
        Thread.sleep(500);
        System.err.println("Wait for all the dataflow master shutdown");
      }
      dataflowRegistry.setStatus(DataflowLifecycleStatus.TERMINATED);
      //Node statusNode = registry.get(dataflowPath);
      //Node dataflowNode = statusNode.getParentNode() ;
      DataflowDescriptor dataflowDescriptor = dataflowRegistry.getDataflowDescriptor();
      scribenginService.moveToHistory(dataflowDescriptor) ;
    }
  }
}
