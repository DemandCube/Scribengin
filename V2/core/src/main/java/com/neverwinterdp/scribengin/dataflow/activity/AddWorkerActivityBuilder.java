package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.SequenceIdTracker;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepBuilder;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.registry.event.WaitingRandomNodeEventListener;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.scribengin.dataflow.worker.VMDataflowWorkerApp;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class AddWorkerActivityBuilder extends ActivityBuilder {
  public Activity build(int numOfWorkerToAdd) {
    Activity activity = new Activity();
    activity.setDescription("Add Dataflow Worker Activity");
    activity.setType("add-dataflow-worker");
    activity.withCoordinator(DataflowActivityCoordinator.class);
    activity.withActivityStepBuilder(AddDataflowWorkerActivityStepBuilder.class) ;
    activity.attribute("num-of-worker-to-add", numOfWorkerToAdd);
    return activity;
  }
  
  @Singleton
  static public class AddDataflowWorkerActivityStepBuilder implements ActivityStepBuilder {
    @Inject
    private Registry registry ;
    
    @Override
    public List<ActivityStep> build(Activity activity, Injector container) throws Exception {
      List<ActivityStep> steps = new ArrayList<>() ;
      int numOfWorkerToAdd = activity.attributeAsInt("num-of-worker-to-add", 0);
      for(int i = 0; i < numOfWorkerToAdd; i++) {
        steps.add(createAddDataflowWorkerStep(registry));
      }
      steps.add(new ActivityStep().
          withType("wait-for-worker-run-status").
          withExecutor(WaitForWorkerRunningStatus.class));
      return steps;
    }
    
    static public ActivityStep createAddDataflowWorkerStep(Registry registry) throws Exception {
      SequenceIdTracker dataflowMasterIdTracker = new SequenceIdTracker(registry, ScribenginService.DATAFLOW_WORKER_ID_TRACKER);
      ActivityStep step = new ActivityStep().
      withType("create-dataflow-worker").
      withExecutor(AddDataflowWorkerStepExecutor.class).
      attribute("worker.id", dataflowMasterIdTracker.nextInt());
      return step;
    }
  }
  
  @Singleton
  static public class AddDataflowWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) throws Exception {
      DataflowDescriptor dflDescriptor = service.getDataflowRegistry().getDataflowDescriptor();

      DataflowRegistry dataflowRegistry = service.getDataflowRegistry();
      Registry registry = dataflowRegistry.getRegistry();
      RegistryConfig registryConfig = registry.getRegistryConfig();

      VMConfig vmConfig = new VMConfig();
      vmConfig.
      setEnvironment(service.getVMConfig().getEnvironment()).
      setName(dflDescriptor.getId() + "-worker-" + step.attribute("worker.id")).
      addRoles("dataflow-worker").
      setRegistryConfig(registryConfig).
      setVmApplication(VMDataflowWorkerApp.class.getName()).
      addProperty("dataflow.registry.path", dataflowRegistry.getDataflowPath()).
      setHadoopProperties(service.getVMConfig().getHadoopProperties());

      String dataflowAppHome = dflDescriptor.getDataflowAppHome();
      if(dataflowAppHome != null) {
        vmConfig.setAppHome(dataflowAppHome);
        vmConfig.addVMResource("dataflow.libs", dataflowAppHome + "/libs");
      }

      VMClient vmClient = new VMClient(registry);
      VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
      service.addWorker(vmDescriptor);
    }
  }
  
  @Singleton
  static public class WaitForWorkerRunningStatus implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(ActivityExecutionContext ctx, Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      Node activeWorkerNodes = dflRegistry.getAllWorkersNode() ;
      List<String> workers = activeWorkerNodes.getChildren();
      WaitingNodeEventListener waitingListener = new WaitingRandomNodeEventListener(dflRegistry.getRegistry()) ;
      for(int i = 0; i < workers.size(); i++) {
        String path = activeWorkerNodes.getPath() + "/" + workers.get(i) + "/status" ;
        step.addLog("");
        String logDesc = "Wait for RUNNING status for worker " + workers.get(i) ;
        step.addLog(logDesc);
        waitingListener.add(path, DataflowWorkerStatus.RUNNING, logDesc, true);
      }
      
      waitingListener.waitForEvents(30 * 1000);
    }
  }
}
