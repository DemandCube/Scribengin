package com.neverwinterdp.vm.service;

import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.mycila.guice.ext.closeable.CloseableModule;
import com.mycila.guice.ext.jsr250.Jsr250Module;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.module.MycilaJmxModuleExt;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.event.VMShutdownEventListener;


public class VMServiceApp extends VMApp {
  private LeaderElection election ;
  
  private Injector  appContainer ;
  private VMService vmService;
  
  public VMService getVMService() { return this.vmService; }
 
  @Override
  public void run() throws Exception {
    election = new LeaderElection(getVM().getVMRegistry().getRegistry(), VMService.LEADER_PATH) ;
    election.setListener(new VMServiceLeaderElectionListener());
    election.start();
    Registry registry = getVM().getVMRegistry().getRegistry();
    VMShutdownEventListener shutdownListener = new VMShutdownEventListener(registry) {
      @Override
      public void onShutdownEvent() throws Exception {
        notifyShutdown();
      }
    };
    try {
      waitForShutdown();
    } catch(InterruptedException ex) {
    } finally {
      if(election != null && election.getLeaderId() != null) {
        election.stop();
      }
      
      if(vmService != null) {
        //TODO: should check to make sure the resource are clean before destroy the service
        Thread.sleep(3000);
        vmService.shutdown();
      }
    }
  }
  
  class VMServiceLeaderElectionListener implements LeaderElectionListener {
    @Override
    public void onElected() {
      try {
        final Registry registry = getVM().getVMRegistry().getRegistry();
        RefNode refNode = new RefNode();
        refNode.setPath(getVM().getDescriptor().getStoredPath());
        registry.setData(VMService.LEADER_PATH, refNode);
        AppModule module = new AppModule(getVM().getDescriptor().getVmConfig().getProperties()) {
          @Override
          protected void configure(Map<String, String> properties) {
            bindInstance(VMConfig.class, getVM().getDescriptor().getVmConfig());
            bindInstance(RegistryConfig.class, registry.getRegistryConfig());
            try {
              bindType(Registry.class, registry.getClass().getName());
            } catch (Throwable e) {
              //TODO: use logger
              e.printStackTrace();
            }
          };
        };
        Module[] modules = {
          new CloseableModule(),new Jsr250Module(), 
          new MycilaJmxModuleExt(getVM().getDescriptor().getVmConfig().getName()), 
          module
        };
        appContainer = Guice.createInjector(Stage.PRODUCTION, modules);
        vmService = appContainer.getInstance(VMService.class);
        vmService.setStatus(VMService.Status.RUNNING);
        VMDescriptor[] vmDescriptor = vmService.getAllocatedVMDescriptors();
        for(VMDescriptor sel : vmDescriptor) {
          if(vmService.isRunning(sel)) vmService.watch(sel);
          else vmService.unregister(sel);
        }
      } catch(Throwable e) {
        e.printStackTrace();
      }
    }
  }
}