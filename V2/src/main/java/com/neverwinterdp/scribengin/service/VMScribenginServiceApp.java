package com.neverwinterdp.scribengin.service;

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
import com.neverwinterdp.vm.client.VMClient;

public class VMScribenginServiceApp extends VMApp {
  private LeaderElection election ;
  private Injector  appContainer ;
  private ScribenginService scribenginMaster;
  
  public ScribenginService getScribenginMaster() { return this.scribenginMaster ; }
  
  @Override
  public void run() throws Exception {
    getVM().getVMRegistry().getRegistry().createIfNotExist(ScribenginService.LEADER_PATH) ;
    election = new LeaderElection(getVM().getVMRegistry().getRegistry(), ScribenginService.LEADER_PATH) ;
    election.setListener(new MasterLeaderElectionListener());
    election.start();
    try {
      //waitForShutdown();
      Thread.sleep(100000000);
    } catch(InterruptedException ex) {
    } finally {
      if(election != null && election.getLeaderId() != null) {
        election.stop();
      }
    }
  }
 
  class MasterLeaderElectionListener implements LeaderElectionListener {
    @Override
    public void onElected() {
      try {
        final Registry registry = getVM().getVMRegistry().getRegistry();
        AppModule module = new AppModule(getVM().getDescriptor().getVmConfig().getProperties()) {
          @Override
          protected void configure(Map<String, String> properties) {
            bindInstance(RegistryConfig.class, registry.getRegistryConfig());
            try {
              bindType(Registry.class, registry.getClass().getName());
              bindInstance(VMConfig.class, getVM().getDescriptor().getVmConfig());
              bindInstance(VMClient.class, new VMClient(registry));
            } catch (ClassNotFoundException e) {
              //TODO: use logger
              e.printStackTrace();
            }
          };
        };
        appContainer = Guice.createInjector(module);
        scribenginMaster = appContainer.getInstance(ScribenginService.class);
        RefNode refNode = new RefNode() ;
        refNode.setPath(getVM().getDescriptor().getStoredPath());
        registry.setData(ScribenginService.LEADER_PATH, refNode);
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
}