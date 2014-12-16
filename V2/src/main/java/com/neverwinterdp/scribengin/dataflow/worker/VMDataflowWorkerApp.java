package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;


public class VMDataflowWorkerApp extends VMApp {
  private Logger logger = LoggerFactory.getLogger(VMDataflowWorkerApp.class) ;
  
  private DataflowContainer container;
  private DataflowWorker dataflowWorker;
  
  public DataflowWorker getDataflowWorker() { return this.dataflowWorker; }
  
  @Override
  public void run() throws Exception {
    VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    final Registry registry = getVM().getVMRegistry().getRegistry();
    AppModule module = new AppModule(vmConfig.getProperties()) {
      @Override
      protected void configure(Map<String, String> properties) {
        bindInstance(RegistryConfig.class, registry.getRegistryConfig());
        try {
          bindType(Registry.class, registry.getClass().getName());
          FileSystem fs = FileSystem.getLocal(new Configuration()) ;
          bindInstance(FileSystem.class, fs);
        } catch (Exception e) {
          logger.error("Error:", e);;
        }
      };
    };
    Injector injector = Guice.createInjector(module);
    container = injector.getInstance(DataflowContainer.class);
    dataflowWorker = container.getInstance(DataflowWorker.class);
    
    try {
      waitForShutdown();
    } catch(InterruptedException ex) {
    } finally {
    }
  }
}