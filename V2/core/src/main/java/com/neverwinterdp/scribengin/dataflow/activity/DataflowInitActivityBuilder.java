package com.neverwinterdp.scribengin.dataflow.activity;

import java.util.HashMap;
import java.util.Map;

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
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.service.DataflowService;
import com.neverwinterdp.scribengin.dataflow.worker.VMDataflowWorkerApp;
import com.neverwinterdp.scribengin.storage.StorageDescriptor;
import com.neverwinterdp.scribengin.storage.sink.Sink;
import com.neverwinterdp.scribengin.storage.sink.SinkFactory;
import com.neverwinterdp.scribengin.storage.source.Source;
import com.neverwinterdp.scribengin.storage.source.SourceFactory;
import com.neverwinterdp.scribengin.storage.source.SourceStream;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowInitActivityBuilder extends ActivityBuilder {
  public DataflowInitActivityBuilder( DataflowDescriptor dflDescriptor) {
    getActivity().setDescription("Init Dataflow Activity");
    getActivity().setType("init-dataflow");
    getActivity().withCoordinator(InitActivityCoordinator.class);
    
    add(new ActivityStep().
        withType("init-dataflow-task").
        withExecutor(InitDataflowTaskExecutor.class));
    
    for(int i = 0; i < dflDescriptor.getNumberOfWorkers(); i++) {
      add(new ActivityStep().
          withType("create-dataflow-worker").
          withExecutor(AddDataflowWorkerStepExecutor.class).
          attribute("worker.id", (i +1)));
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
  static public class InitDataflowTaskExecutor implements ActivityStepExecutor {
    @Inject
    private DataflowService service ;

    @Override
    public void execute(Activity activity, ActivityStep step) {
      try {
        DataflowDescriptor dataflowDescriptor = service.getDataflowRegistry().getDataflowDescriptor();
        SourceFactory sourceFactory = service.getSourceFactory();
        SinkFactory sinkFactory = service.getSinkFactory() ;

        Source source    = sourceFactory.create(dataflowDescriptor.getSourceDescriptor()) ;
        Map<String, Sink> sinks = new HashMap<String, Sink>();
        for(Map.Entry<String, StorageDescriptor> entry : dataflowDescriptor.getSinkDescriptors().entrySet()) {
          Sink sink = sinkFactory.create(entry.getValue());
          sinks.put(entry.getKey(), sink);
        }

        SourceStream[] sourceStream = source.getStreams();
        for(int i = 0; i < sourceStream.length; i++) {
          DataflowTaskDescriptor descriptor = new DataflowTaskDescriptor();
          descriptor.setId(i);
          descriptor.setScribe(dataflowDescriptor.getScribe());
          descriptor.setSourceStreamDescriptor(sourceStream[i].getDescriptor());
          for(Map.Entry<String, Sink> entry : sinks.entrySet()) {
            descriptor.add(entry.getKey(), entry.getValue().newStream().getDescriptor());
          }
          service.addAvailableTask(descriptor);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
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
        VMClient vmClient = new VMClient(registry);
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

        VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
        service.addWorker(vmDescriptor);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
