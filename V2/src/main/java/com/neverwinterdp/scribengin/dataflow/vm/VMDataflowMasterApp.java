package com.neverwinterdp.scribengin.dataflow.vm;

import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
import com.neverwinterdp.scribengin.dataflow.DataflowMaster;
import com.neverwinterdp.vm.VMApp;
import com.neverwinterdp.vm.VMConfig;


public class VMDataflowMasterApp extends VMApp {
  private String         dataflowRegistryPath;
  private LeaderElection election;
  private DataflowMaster dataflowMaster;
  private Injector       appContainer;

  @Override
  public void run() throws Exception {
    VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    dataflowRegistryPath = vmConfig.getProperties().get("dataflow.registry.path");
    election = new LeaderElection(getVM().getVMRegistry().getRegistry(), dataflowRegistryPath + "/master/leader") ;
    election.setListener(new MasterLeaderElectionListener());
    election.start();
    try {
      waitForShutdown();
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
        registry.setData(dataflowRegistryPath + "/master/leader", getVM().getDescriptor());
        AppModule module = new AppModule(getVM().getDescriptor().getVmConfig().getProperties()) {
          @Override
          protected void configure(Map<String, String> properties) {
            bindInstance(RegistryConfig.class, registry.getRegistryConfig());
            try {
              bindType(Registry.class, registry.getClass().getName());
            } catch (ClassNotFoundException e) {
              //TODO: use logger
              e.printStackTrace();
            }
          };
        };
        appContainer = Guice.createInjector(module);
        dataflowMaster = appContainer.getInstance(DataflowMaster.class);
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
}