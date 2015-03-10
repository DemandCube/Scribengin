package com.neverwinterdp.scribengin.dataflow.service;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

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


public class VMDataflowServiceApp extends VMApp {
  private String         dataflowRegistryPath;
  private LeaderElection election;
  private DataflowService dataflowService;
  private Injector       appContainer;
  private ServiceRunnerThread serviceRunnerThread;
  
  @Override
  public void run() throws Exception {
    VMConfig vmConfig = getVM().getDescriptor().getVmConfig();
    dataflowRegistryPath = vmConfig.getProperties().get("dataflow.registry.path");
    election = new LeaderElection(getVM().getVMRegistry().getRegistry(), dataflowRegistryPath + "/master/leader") ;
    election.setListener(new MasterLeaderElectionListener());
    election.start();
    try {
      waitForShutdown();
      System.err.println("finish waitForShutdown()");
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
              vmConfig.overrideHadoopConfiguration(conf);
              FileSystem fs = FileSystem.get(conf);
              bindInstance(FileSystem.class, fs);
            } catch (Exception e) {
              //TODO: use logger
              e.printStackTrace();
            }
          };
        };
        RefNode leaderRefNode = new RefNode();
        leaderRefNode.setPath(getVM().getDescriptor().getStoredPath());
        registry.setData(dataflowRegistryPath + "/master/leader", leaderRefNode);
        appContainer = Guice.createInjector(module);
        dataflowService = appContainer.getInstance(DataflowService.class);
        serviceRunnerThread = new ServiceRunnerThread(dataflowService);
        serviceRunnerThread.start();
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  public class ServiceRunnerThread extends Thread {
    DataflowService service;
    
    ServiceRunnerThread(DataflowService service) {
      this.service = service;
    }
    
    public void run() {
      try {
        service.run();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        System.err.println("ServiceRunnerThread: notifyShutdown()");
        notifyShutdown();
      }
    }
  }
}