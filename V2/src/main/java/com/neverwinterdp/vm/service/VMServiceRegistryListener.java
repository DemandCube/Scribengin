package com.neverwinterdp.vm.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.election.RegistryLeaderElectionListener;
import com.neverwinterdp.vm.VMDescriptor;

public class VMServiceRegistryListener {
  private Logger logger = LoggerFactory.getLogger(VMServiceRegistryListener.class) ;
  
  private Registry registry;
  private RegistryLeaderElectionListener<VMDescriptor> leaderListener;
  
  public VMServiceRegistryListener(Registry registry) throws RegistryException {
    this.registry = registry;
    leaderListener = 
        new RegistryLeaderElectionListener<VMDescriptor>(registry, VMDescriptor.class, VMService.LEADER_PATH);
  }
  
  public void add(RegistryLeaderElectionListener.LeaderListener<VMDescriptor> listener) {
    leaderListener.add(listener);
  }
}
