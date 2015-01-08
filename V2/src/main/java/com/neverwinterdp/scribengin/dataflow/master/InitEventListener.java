package com.neverwinterdp.scribengin.dataflow.master;

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

public class InitEventListener implements EventListener {

  @Override
  public void onEvent(DataflowMaster master, Event event) throws Exception {
    if(event !=  Event.INIT) return;
    
    DataflowRegistry dataflowRegistry = master.getDataflowRegistry();
    DataflowDescriptor dataflowDescriptor = dataflowRegistry.getDataflowDescriptor();
    dataflowRegistry.createRegistryStructure();
    
    initTaskDescriptors(master, dataflowDescriptor);
    initWorkers(master, dataflowDescriptor);
  }
  
  private void initTaskDescriptors(DataflowMaster master, DataflowDescriptor dataflowDescriptor) throws Exception {
    DataflowRegistry dataflowRegistry = master.getDataflowRegistry();
    SourceFactory sourceFactory = master.getSourceFactory();
    SinkFactory sinkFactory = master.getSinkFactory() ;
    
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
      dataflowRegistry.addAvailable(descriptor);
    }
  }
  
  private void initWorkers(DataflowMaster master, DataflowDescriptor dataflowDescriptor) throws Exception {
    DataflowRegistry dataflowRegistry = master.getDataflowRegistry();
    Registry registry = dataflowRegistry.getRegistry();
    VMClient vmClient = new VMClient(registry);
    RegistryConfig registryConfig = registry.getRegistryConfig();
    for(int i = 0; i < dataflowDescriptor.getNumberOfWorkers(); i++) {
      VMConfig vmConfig = 
        new VMConfig().
        setEnvironment(master.getVMConfig().getEnvironment()).
        setName(dataflowDescriptor.getName() + "-worker-" + (i + 1)).
        addRoles("dataflow-worker").
        setRegistryConfig(registryConfig).
        setVmApplication(VMDataflowWorkerApp.class.getName()).
        addProperty("dataflow.registry.path", dataflowRegistry.getDataflowPath()).
        setYarnConf(master.getVMConfig().getYarnConf());
        VMDescriptor vmDescriptor = vmClient.allocate(vmConfig);
      dataflowRegistry.addWorker(vmDescriptor);
    }
  }
}
