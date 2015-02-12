package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
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
import com.neverwinterdp.vm.VMDescriptor;


public class VMDataflowWorkerApp extends VMApp {
  private Logger logger = LoggerFactory.getLogger(VMDataflowWorkerApp.class) ;
  
  private DataflowContainer container;
  private DataflowTaskExecutorManager dataflowTaskExecutorManager;
  
  public DataflowTaskExecutorManager getDataflowWorker() { return this.dataflowTaskExecutorManager; }
  
  @Override
  public void run() throws Exception {
    final VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    AppModule module = new AppModule(vmConfig.getProperties()) {
      @Override
      protected void configure(Map<String, String> properties) {
        Registry registry = getVM().getVMRegistry().getRegistry();
        bindInstance(RegistryConfig.class, registry.getRegistryConfig());
        bindInstance(VMDescriptor.class, getVM().getDescriptor());
        try {
          bindType(Registry.class, registry.getClass().getName());
          FileSystem fs = null; 
          VMConfig.Environment env = vmConfig.getEnvironment();
          if(env == VMConfig.Environment.YARN || env == VMConfig.Environment.YARN_MINICLUSTER) {
            YarnConfiguration conf = new YarnConfiguration();
            vmConfig.overrideHadoopConfiguration(conf);
            fs = FileSystem.get(conf) ;
          } else {
            fs = FileSystem.getLocal(new Configuration());
          }
          bindInstance(FileSystem.class, fs);
        } catch (Exception e) {
          logger.error("Error:", e);;
        }
      };
    };
    Injector injector = Guice.createInjector(module);
    container = injector.getInstance(DataflowContainer.class);
    dataflowTaskExecutorManager = container.getDataflowTaskExecutorManager();
    try {
      dataflowTaskExecutorManager.waitForExecutorTermination(5000);
    } catch(InterruptedException ex) {
    } finally {
      dataflowTaskExecutorManager.shutdown();
    }
  }
}