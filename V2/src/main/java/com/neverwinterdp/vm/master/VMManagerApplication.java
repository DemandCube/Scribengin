package com.neverwinterdp.vm.master;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.election.LeaderElection;
import com.neverwinterdp.registry.election.LeaderElectionListener;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.VMApplication;
import com.neverwinterdp.vm.VMService;


abstract public class VMManagerApplication extends VMApplication {
  private LeaderElection election ;
  
  private Injector  appContainer ;
  private VMService vmService;
  private Registry  registry;
  
  
  public VMService getVMService() { return this.vmService; }
 
  @Override
  public void onInit(Map<String, String> props) throws Exception {
    Map<String, String> defaultProps = new HashMap<String, String>();
    defaultProps.put("registry.connect", "127.0.0.1:2181") ;
    defaultProps.put("registry.db-domain", "/NeverwinterDP") ;
    defaultProps.put("implementation:" + Registry.class.getName(), RegistryImpl.class.getName()) ;
    if(props != null) defaultProps.putAll(props);
    
    AppModule module = new AppModule(defaultProps) {
      @Override
      protected void configure(Map<String, String> properties) {
        onInit(this);
      };
    };
    appContainer = Guice.createInjector(module);
    registry =  appContainer.getInstance(Registry.class);
    vmService = appContainer.getInstance(VMService.class);
    vmService.allocate(getVM());
  }
  
  abstract protected void onInit(AppModule module) ;
  
  @Override
  public void onDestroy() throws Exception {
  }
  
  @Override
  public void run() throws Exception {
    election = new LeaderElection(registry, VMService.LEADER_PATH) ;
    election.setListener(new MasterLeaderElectionListener());
    election.start();
    
    try {
      Thread.sleep(10000000);
    } catch(InterruptedException ex) {
      election.stop();
    }
  }
  
  class MasterLeaderElectionListener implements LeaderElectionListener {
    @Override
    public void onElected() {
      try {
        registry.setData(VMService.LEADER_PATH, getVM().getDescriptor());
      } catch(Exception e) {
        e.printStackTrace();
      }
    }
  }
}