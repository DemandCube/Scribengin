package com.neverwinterdp.vm.master;

import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMService;


public class VMManagerApp extends VMApp {
  private LeaderElection election ;
  
  private Injector  appContainer ;
  private VMService vmService;
  
  public VMService getVMService() { return this.vmService; }
 
  @Override
  public void run() throws Exception {
    election = new LeaderElection(getVM().getVMRegistry().getRegistry(), VMService.LEADER_PATH) ;
    election.setListener(new MasterLeaderElectionListener());
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
  
  class MasterLeaderElectionListener implements LeaderElectionListener {
    @Override
    public void onElected() {
      try {
        final Registry registry = getVM().getVMRegistry().getRegistry();
        registry.setData(VMService.LEADER_PATH, getVM().getDescriptor());
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
        System.out.println("Before get VMService") ;
        vmService = appContainer.getInstance(VMService.class);
        System.out.println("After get VMService = " + vmService) ;
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