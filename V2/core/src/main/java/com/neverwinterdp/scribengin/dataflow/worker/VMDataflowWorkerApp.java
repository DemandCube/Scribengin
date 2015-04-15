package com.neverwinterdp.scribengin.dataflow.worker;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.mycila.guice.ext.closeable.CloseableInjector;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.module.MycilaJmxModuleExt;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.scribengin.dataflow.DataflowContainer;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;


public class VMDataflowWorkerApp extends VMApp {
  private Logger logger = LoggerFactory.getLogger(VMDataflowWorkerApp.class) ;
  
  private DataflowContainer container;
  private DataflowTaskExecutorService dataflowTaskExecutorService;
  
  @Override
  public void run() throws Exception {
    final VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    AppModule module = new AppModule(vmConfig.getProperties()) {
      @Override
      protected void configure(Map<String, String> properties) {
        Registry registry = getVM().getVMRegistry().getRegistry();
        bindInstance(RegistryConfig.class, registry.getRegistryConfig());
        bindType(Registry.class, registry.getClass());
        bindInstance(VMDescriptor.class, getVM().getDescriptor());
        try {
          VMConfig.Environment env = vmConfig.getEnvironment();
          if(env == VMConfig.Environment.YARN || env == VMConfig.Environment.YARN_MINICLUSTER) {
            YarnConfiguration conf = new YarnConfiguration();
            vmConfig.overrideHadoopConfiguration(conf);
            bindInstance(FileSystem.class, FileSystem.get(conf));
          } else {
            bindInstance(FileSystem.class, FileSystem.getLocal(new Configuration()));
          }
        } catch (Exception e) {
          logger.error("Error:", e);;
        }
      };
    };
    Module[] modules = {
      new CloseableModule(),new Jsr250Module(), 
      new MycilaJmxModuleExt(getVM().getDescriptor().getVmConfig().getName()), 
      module
    };
    Injector injector = Guice.createInjector(Stage.PRODUCTION, modules);
    container = injector.getInstance(DataflowContainer.class);
    dataflowTaskExecutorService = container.getDataflowTaskExecutorManager();
    addListener(new VMApp.EventListener() {
      @Override
      public void onEvent(VMApp vmApp, Event event) {
        try {
          if(event == Event.Shutdown) {
            dataflowTaskExecutorService.shutdown();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    try {
      dataflowTaskExecutorService.start();
      dataflowTaskExecutorService.waitForFinish(1000);
    } catch(InterruptedException ex) {
    } finally {
      dataflowTaskExecutorService.shutdown();
      container.getInstance(CloseableInjector.class).close();
      System.err.println("Shutdown dataflow worker....................");
    }
  }
}