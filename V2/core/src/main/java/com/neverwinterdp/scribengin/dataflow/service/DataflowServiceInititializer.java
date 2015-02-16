package com.neverwinterdp.scribengin.dataflow.service;

import java.util.HashMap;
import java.util.Map;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.worker.VMDataflowWorkerApp;
import com.neverwinterdp.scribengin.sink.Sink;
import com.neverwinterdp.scribengin.sink.SinkDescriptor;
import com.neverwinterdp.scribengin.sink.SinkFactory;
import com.neverwinterdp.scribengin.source.Source;
import com.neverwinterdp.scribengin.source.SourceFactory;
import com.neverwinterdp.scribengin.source.SourceStream;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

public class DataflowServiceInititializer {

  public void onInit(DataflowService service) throws Exception {
    DataflowRegistry dataflowRegistry = service.getDataflowRegistry();
    DataflowDescriptor dataflowDescriptor = dataflowRegistry.getDataflowDescriptor();
    initTaskDescriptors(service, dataflowDescriptor);
    initWorkers(service, dataflowDescriptor);
  }
  
  private void initTaskDescriptors(DataflowService service, DataflowDescriptor dataflowDescriptor) throws Exception {
    SourceFactory sourceFactory = service.getSourceFactory();
    SinkFactory sinkFactory = service.getSinkFactory() ;
    
    Source source    = sourceFactory.create(dataflowDescriptor.getSourceDescriptor()) ;
    Map<String, Sink> sinks = new HashMap<String, Sink>();
    for(Map.Entry<String, SinkDescriptor> entry : dataflowDescriptor.getSinkDescriptors().entrySet()) {
      Sink sink = sinkFactory.create(entry.getValue());
      sinks.put(entry.getKey(), sink);
    }
    
    SourceStream[] sourceStream = source.getStreams();
    for(int i = 0; i < sourceStream.length; i++) {
      DataflowTaskDescriptor descriptor = new DataflowTaskDescriptor();
      descriptor.setId(i);
      descriptor.setDataProcessor(dataflowDescriptor.getDataProcessor());
      descriptor.setSourceStreamDescriptor(sourceStream[i].getDescriptor());
      for(Map.Entry<String, Sink> entry : sinks.entrySet()) {
        descriptor.add(entry.getKey(), entry.getValue().newStream().getDescriptor());
      }
      service.addAvailableTask(descriptor);
    }
  }
  
  private void initWorkers(DataflowService service, DataflowDescriptor dataflowDescriptor) throws Exception {
    DataflowRegistry dataflowRegistry = service.getDataflowRegistry();
    Registry registry = dataflowRegistry.getRegistry();
    VMClient vmClient = new VMClient(registry);
    RegistryConfig registryConfig = registry.getRegistryConfig();
    String dataflowAppHome = dataflowDescriptor.getDataflowAppHome();
    for(int i = 0; i < dataflowDescriptor.getNumberOfWorkers(); i++) {
      VMConfig vmConfig = 
        new VMConfig().
        setEnvironment(service.getVMConfig().getEnvironment()).
        setName(dataflowDescriptor.getName() + "-worker-" + (i + 1)).
        addRoles("dataflow-worker").
        setRegistryConfig(registryConfig).
        setVmApplication(VMDataflowWorkerApp.class.getName()).
        addProperty("dataflow.registry.path", dataflowRegistry.getDataflowPath()).
        setHadoopProperties(service.getVMConfig().getHadoopProperties());
      if(dataflowAppHome != null) {
        vmConfig.setAppHome(dataflowAppHome);
        vmConfig.addVMResource("dataflow.libs", dataflowAppHome + "/libs");
      }
      VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
      service.addWorker(vmDescriptor);
    }
  }
}
