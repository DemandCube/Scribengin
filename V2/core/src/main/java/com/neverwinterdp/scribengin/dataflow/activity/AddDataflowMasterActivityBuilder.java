package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.registry.event.WaitingRandomNodeEventListener;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.service.VMDataflowServiceApp;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class AddDataflowMasterActivityBuilder extends ActivityBuilder {
  static public AtomicInteger idTracker = new AtomicInteger(1) ;
  
  public Activity build(int numOfWorkerToAdd) {
    Activity activity = new Activity();
    activity.setDescription("Add Dataflow Master Activity");
    activity.setType("add-dataflow-master");
    activity.withCoordinator(AddDataflowMasterActivityCoordinator.class);
    activity.withActivityStepBuilder(AddDataflowMasterActivityStepBuilder.class) ;
    return activity;
  }
  
  @Singleton
  static public class AddDataflowMasterActivityStepBuilder implements ActivityStepBuilder {
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      steps.add(new ActivityStep().
          withType("create-dataflow-master").
          withExecutor(AddDataflowMasterStepExecutor.class).
          attribute("master.id", idTracker.getAndIncrement()));
      steps.add(new ActivityStep().
          withType("wait-for-master-run-status").
          withExecutor(WaitForDataflowMasterRunningStatus.class));
      return steps;
    }
  }
  
  @Singleton
  static public class AddDataflowMasterActivityCoordinator extends ActivityCoordinator {
    @Inject
    DataflowActivityStepWorkerService activityStepWorkerService;
   
    @Override
    protected <T> void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
      activityStepWorkerService.exectute(context, activity, step);
    }
  }
  
  @Singleton
  static public class AddDataflowMasterStepExecutor implements ActivityStepExecutor {
    @Inject
    private Registry registry ;
    
    @Inject
    private VMConfig vmConfig;
    
    @Override
    public void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
      String dataflowDescriptorPath = activity.attribute("dataflow.descriptor.path");
      Node dataflowDescriptorNode = registry.get(dataflowDescriptorPath) ;
      DataflowDescriptor descriptor = dataflowDescriptorNode.getDataAs(DataflowDescriptor.class);
      String dataflowAppHome = descriptor.getDataflowAppHome();
      Node dataflowNode = registry.get(ScribenginService.DATAFLOWS_RUNNING_PATH + "/" + descriptor.getName()) ;
      VMConfig dfVMConfig = new VMConfig() ;
      if(dataflowAppHome != null) {
        dfVMConfig.setAppHome(dataflowAppHome);
        dfVMConfig.addVMResource("dataflow.libs", dataflowAppHome + "/libs");
      }
      dfVMConfig.setEnvironment(vmConfig.getEnvironment());
      dfVMConfig.setName(descriptor.getName() + "-master-" + idTracker.incrementAndGet());
      dfVMConfig.setRoles(Arrays.asList("dataflow-master"));
      dfVMConfig.setRegistryConfig(registry.getRegistryConfig());
      dfVMConfig.setVmApplication(VMDataflowServiceApp.class.getName());
      dfVMConfig.addProperty("dataflow.registry.path", dataflowNode.getPath());
      dfVMConfig.setHadoopProperties(vmConfig.getHadoopProperties());
      VMClient vmClient = new VMClient(registry);
      VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
    }
  }
  
  @Singleton
  static public class WaitForDataflowMasterRunningStatus implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      Node activeWorkerNodes = dflRegistry.getActiveWorkersNode() ;
      List<String> workers = activeWorkerNodes.getChildren();
      WaitingNodeEventListener waitingListener = new WaitingRandomNodeEventListener(dflRegistry.getRegistry()) ;
      for(int i = 0; i < workers.size(); i++) {
        String path = activeWorkerNodes.getPath() + "/" + workers.get(i) + "/status" ;
        waitingListener.add(path, DataflowWorkerStatus.RUNNING);
      }
      
      waitingListener.waitForEvents(30 * 1000);
      TabularFormater formater = waitingListener.getTabularFormaterEventLogInfo();
      System.err.println(formater.getFormatText());
    }
  }
}
