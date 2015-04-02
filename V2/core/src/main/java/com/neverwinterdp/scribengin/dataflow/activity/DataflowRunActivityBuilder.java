package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.Random;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityBuilder;
import com.neverwinterdp.registry.activity.ActivityCoordinator;
import com.neverwinterdp.registry.activity.ActivityService;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.registry.activity.ActivityStepExecutor;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.worker.VMDataflowWorkerApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowRunActivityBuilder extends ActivityBuilder {
  static int idTracker = 1 ;
  
  public DataflowRunActivityBuilder( DataflowDescriptor dflDescriptor) {
    getActivity().setDescription("Run Dataflow Activity");
    getActivity().setType("run-dataflow");
    getActivity().withCoordinator(InitActivityCoordinator.class);
    for(int i = 0; i < dflDescriptor.getNumberOfWorkers(); i++) {
      add(new ActivityStep().
          withType("create-dataflow-worker").
          withExecutor(AddDataflowWorkerStepExecutor.class).
          attribute("worker.id", idTracker++));
    }
  }
  
  @Singleton
  static public class InitActivityCoordinator extends ActivityCoordinator {
    @Inject
    DataflowActivityStepWorkerService activityStepWorkerService;
   
    @Override
    protected <T> void execute(ActivityService service, Activity activity, ActivityStep step) {
      activityStepWorkerService.exectute(activity, step);
    }
  }
  
  @Singleton
  static public class AddDataflowWorkerStepExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;
    
    @Override
    public void execute(Activity activity, ActivityStep step) {
      try {
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
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
