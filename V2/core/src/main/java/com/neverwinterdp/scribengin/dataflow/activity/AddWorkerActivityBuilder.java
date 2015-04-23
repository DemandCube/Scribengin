package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityExecutionContext;
import com.neverwinterdp.registry.activity.ActivityService;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.registry.event.WaitingRandomNodeEventListener;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.worker.DataflowWorkerStatus;
import com.neverwinterdp.scribengin.dataflow.worker.VMDataflowWorkerApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class AddWorkerActivityBuilder extends ActivityBuilder {
  static public AtomicInteger idTracker = new AtomicInteger(1) ;
  
  public AddWorkerActivityBuilder() {
  }
  
  public AddWorkerActivityBuilder(int numOfWorkerToAdd) {
    getActivity().setDescription("Add Dataflow Worker Activity");
    getActivity().setType("add-dataflow-worker");
    getActivity().withCoordinator(AddDataflowWorkerActivityCoordinator.class);
    
    for(int i = 0; i < numOfWorkerToAdd; i++) {
      add(new ActivityStep().
          withType("create-dataflow-worker").
          withExecutor(AddDataflowWorkerStepExecutor.class).
          attribute("worker.id", idTracker.getAndIncrement()));
    }
    add(new ActivityStep().
        withType("wait-for-worker-run-status").
        withExecutor(WaitForWorkerRunningStatus.class));
  }
  
  @Singleton
  static public class AddDataflowWorkerActivityCoordinator extends ActivityCoordinator {
    @Inject
    DataflowActivityStepWorkerService activityStepWorkerService;
   
    @Override
    protected <T> void execute(ActivityExecutionContext context, Activity activity, ActivityStep step) {
      activityStepWorkerService.exectute(activity, step);
    }
  }
  
  @Singleton
  static public class AddDataflowWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(Activity activity, ActivityStep step) throws Exception {
      DataflowDescriptor dflDescriptor = service.getDataflowRegistry().getDataflowDescriptor();

      DataflowRegistry dataflowRegistry = service.getDataflowRegistry();
      Registry registry = dataflowRegistry.getRegistry();
      RegistryConfig registryConfig = registry.getRegistryConfig();

      VMConfig vmConfig = new VMConfig();
      vmConfig.
      setEnvironment(service.getVMConfig().getEnvironment()).
      setName(dflDescriptor.getName() + "-worker-" + step.attribute("worker.id")).
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
    public void execute(Activity activity, ActivityStep step) throws Exception {
      DataflowRegistry dflRegistry = service.getDataflowRegistry();
      Node workerNodes = dflRegistry.getActiveWorkersNode() ;
      List<String> workers = workerNodes.getChildren();
      WaitingNodeEventListener waitingListener = new WaitingRandomNodeEventListener(dflRegistry.getRegistry()) ;
      for(int i = 0; i < workers.size(); i++) {
        String path = workerNodes.getPath() + "/" + workers.get(i) + "/status" ;
        waitingListener.add(path, DataflowWorkerStatus.RUNNING);
      }
      waitingListener.waitForEvents(30 * 1000);
    }
  }
}
