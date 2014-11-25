package com.neverwinterdp.scribengin.master;

import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
import com.neverwinterdp.scribengin.dataflow.DataflowConfig;
import com.neverwinterdp.scribengin.master.MasterDescriptor.Type;
import com.neverwinterdp.vm.VMService;

//TODO: should we pick the master name or leader name
public class Master {
  final static public String MASTER_PATH = "/master" ;
  
  @Inject
  private MasterConfig config;
  
  @Inject
  private Registry registry ;
  
  @Inject
  private VMService vmService ;
  
  private MasterDescriptor descriptor ;
  private  LeaderElection election  ;

  public Master() {
  }
  
  public MasterDescriptor getDescriptor() { return this.descriptor ; }
  
  public LeaderElection getLeaderElection() { return this.election; }
  
  public void start() throws RegistryException {
    registry.connect() ;
    registry.createIfNotExist(MASTER_PATH);
    descriptor = new MasterDescriptor() ;
    election = new LeaderElection(registry, MASTER_PATH) ;
    election.setListener(new MasterLeaderElectionListener());
    election.start();
    descriptor.setId(Long.toString(election.getLeaderId().getSequence()));
    election.getNode().setData(descriptor);
  }
  
  public void stop() throws Exception, RegistryException {
    if(election != null) {
      election.stop();
      election = null ;
      vmService.shutdown();
      registry.disconnect();
    }
  }
  
  public void submit(DataflowConfig config) throws Exception {
  }
  
  class MasterLeaderElectionListener implements LeaderElectionListener {
    @Override
    public void onElected() {
      try {
        descriptor.setType(Type.LEADER);
        election.getNode().setData(descriptor);
        //vmService.start();
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  static public Master create(Map<String, String> properties) throws Exception {
    AppModule module = new AppModule(properties) {
      protected void configure(Map<String, String> properties) {
        try {
          bindType(Registry.class, properties.get("registry.implementation"));
          bindType(VMService.class, properties.get("vm.implementation"));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    };
   
    Injector container = Guice.createInjector(module);
    
    Master master = container.getInstance(Master.class) ;
    return master ;
  }
}