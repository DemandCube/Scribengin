package com.neverwinterdp.scribengin.dataflow.master;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryConfig;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
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
        final VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
        AppModule module = new AppModule(vmConfig.getProperties()) {
          @Override
          protected void configure(Map<String, String> properties) {
            bindInstance(VMConfig.class, vmConfig);
            bindInstance(RegistryConfig.class, registry.getRegistryConfig());
            try {
              bindType(Registry.class, registry.getClass().getName());
              Configuration conf = new Configuration();
              vmConfig.overrideYarnConfiguration(conf);
              FileSystem fs = FileSystem.get(conf);
              bindInstance(FileSystem.class, fs);
            } catch (Exception e) {
              //TODO: use logger
              e.printStackTrace();
            }
          };
        };
        registry.setData(dataflowRegistryPath + "/master/leader", getVM().getDescriptor());
        appContainer = Guice.createInjector(module);
        dataflowMaster = appContainer.getInstance(DataflowMaster.class);
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
}