package com.neverwinterdp.scribengin.master;

import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.scribengin.dataflow.DataflowConfig;
import com.neverwinterdp.scribengin.master.MasterDescriptor.Type;
import com.neverwinterdp.scribengin.module.ScribenginModule;
import com.neverwinterdp.scribengin.registry.RegistryService;
import com.neverwinterdp.scribengin.registry.RegistryException;
import com.neverwinterdp.scribengin.registry.election.LeaderElection;
import com.neverwinterdp.scribengin.registry.election.LeaderElectionListener;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMService;

//TODO: should we pick the master name or leader name
public class Master {
  final static public String MASTER_PATH = "/master" ;
  
  @Inject
  private MasterConfig config;
  
  @Inject
  private RegistryService registry ;
  
  @Inject
  private VMService vmResourceService ;
  
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
      vmResourceService.stop();
      registry.disconnect();
    }
  }
  
  public void submit(DataflowConfig config) throws Exception {
    VM vmresource = vmResourceService.allocate(1, 128);
  }
  
  class MasterLeaderElectionListener implements LeaderElectionListener {
    @Override
    public void onElected() {
      try {
        descriptor.setType(Type.LEADER);
        election.getNode().setData(descriptor);
        vmResourceService.start();
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
  
  static public Master create(Map<String, String> properties) throws Exception {
    ScribenginModule module = new ScribenginModule(properties) {
      protected void configure(Map<String, String> properties) {
        try {
          bindType(RegistryService.class, properties.get("registry.implementation"));
          bindType(VMService.class, properties.get("vmresource.implementation"));
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