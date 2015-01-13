package com.neverwinterdp.vm.service;

import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.RefNode;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;


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
    try {
      waitForShutdown();
    } catch(InterruptedException ex) {
    } finally {
      if(vmService != null) vmService.close();
      if(election != null && election.getLeaderId() != null) {
        vmService.close();
        election.stop();
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
        appContainer = Guice.createInjector(module);
        vmService = appContainer.getInstance(VMService.class);
        VMDescriptor[] vmDescriptor = vmService.getAllocatedVMDescriptors();
        for(VMDescriptor sel : vmDescriptor) {
          if(vmService.isRunning(sel)) vmService.getVMListenerManager().watch(sel);
          else vmService.unregister(sel);
        }
      } catch(Throwable e) {
        e.printStackTrace();
      }
    }
  }
}