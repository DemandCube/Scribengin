package com.neverwinterdp.scribengin.master;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.scribengin.dataflow.config.DataflowConfig;
import com.neverwinterdp.scribengin.master.MasterDescriptor.Type;
import com.neverwinterdp.scribengin.registry.Registry;
import com.neverwinterdp.scribengin.registry.RegistryConfig;
import com.neverwinterdp.scribengin.registry.RegistryException;
import com.neverwinterdp.scribengin.registry.RegistryFactory;
import com.neverwinterdp.scribengin.registry.election.LeaderElection;
import com.neverwinterdp.scribengin.registry.election.LeaderElectionListener;
import com.neverwinterdp.scribengin.vmresource.VMResourceService;
import com.neverwinterdp.scribengin.vmresource.VMResourceConfig;
import com.neverwinterdp.scribengin.vmresource.VMResourceFactory;

//TODO: should we pick the master name or leader name
public class Master {
  final static public String MASTER_PATH = "/master" ;
  private MasterDescriptor descriptor ;
  private Registry registry ;
  private VMResourceService vmResourceAllocator ;
  private  LeaderElection election  ;

  public Master(String[] args) throws Exception {
    MasterConfig config = new MasterConfig() ;
    new JCommander(config, args) ;
    
    RegistryConfig registryConfig = config.getRegistryConfig();
    Class<RegistryFactory> factory = (Class<RegistryFactory>)Class.forName(registryConfig.getFactory()) ;
    registry = factory.newInstance().create(registryConfig);
    
    VMResourceConfig vmResourceConfig = config.getVmResourceConfig();
    Class<VMResourceFactory> vmResourceFactory = (Class<VMResourceFactory>)Class.forName(vmResourceConfig.getFactory()) ;
    vmResourceAllocator = vmResourceFactory.newInstance().createAllocator(vmResourceConfig);
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
      vmResourceAllocator.stop();
      registry.disconnect();
    }
  }
  
  public void submit(DataflowConfig config) {
  }
  
  class MasterLeaderElectionListener implements LeaderElectionListener {
    @Override
    public void onElected() {
      try {
        descriptor.setType(Type.LEADER);
        election.getNode().setData(descriptor);
        vmResourceAllocator.start();
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
}