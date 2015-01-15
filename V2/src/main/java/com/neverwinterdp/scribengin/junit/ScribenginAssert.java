package com.neverwinterdp.scribengin.junit;

import static com.neverwinterdp.scribengin.junit.ScribenginAssertEvent.*;

import com.neverwinterdp.registry.NodeEvent;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.election.RegistryLeaderElectionListener;
import com.neverwinterdp.scribengin.junit.ScribenginAssertEvent.ScribenginAttr;
import com.neverwinterdp.scribengin.service.ScribenginServiceRegistryListener;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.junit.VMAssert;


public class ScribenginAssert extends VMAssert {
  private ScribenginServiceRegistryListener ssRegistryListener ;
  
  public ScribenginAssert(Registry registry) throws RegistryException {
    super(registry);
    
    ssRegistryListener = new ScribenginServiceRegistryListener(registry) ;
    ssRegistryListener.add(new LeaderListenerImpl());
  }
  
  public void assertScribenginMaster(String desc, String vmName) {
    add(new ScribenginAssertMasterElection(desc, vmName));
  }
  
  public class LeaderListenerImpl implements RegistryLeaderElectionListener.LeaderListener<VMDescriptor> {
    @Override
    public void onElected(NodeEvent event, VMDescriptor learderVMDescriptor) {
      ScribenginAssertEvent saEvent = new ScribenginAssertEvent(SCRIBENGIN_MASTER_ELECTION, event);
      saEvent.attr(ScribenginAttr.master_leader, learderVMDescriptor);
      process(saEvent);
    }
  }
  
  static public class ScribenginAssertMasterElection extends ScribenginAssertUnit {
    String   expectVMName;
    
    public ScribenginAssertMasterElection(String description, String vmName) {
      super(description);
      this.expectVMName = vmName;
    }

    @Override
    public boolean assertEvent(ScribenginAssertEvent event) {
      if(!SCRIBENGIN_MASTER_ELECTION.equals(event.getName())) return false;
      VMDescriptor vmDescriptor = event.attr(ScribenginAttr.master_leader);
      if(!expectVMName.equals(vmDescriptor.getVmConfig().getName())) return false;
      return true;
    }
  }
}
