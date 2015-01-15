package com.neverwinterdp.scribengin.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.registry.election.RegistryLeaderElectionListener;
import com.neverwinterdp.vm.VMDescriptor;

public class ScribenginServiceRegistryListener {
  private Logger logger = LoggerFactory.getLogger(ScribenginServiceRegistryListener.class) ;
  
  private Registry registry ;
  private RegistryLeaderElectionListener<VMDescriptor> leaderListener;
  
  public ScribenginServiceRegistryListener(Registry registry) throws RegistryException {
    this.registry = registry;
    leaderListener = 
      new RegistryLeaderElectionListener<VMDescriptor>(registry, VMDescriptor.class, ScribenginService.LEADER_PATH);
  }
  
  public void add(RegistryLeaderElectionListener.LeaderListener<VMDescriptor> listener) {
    leaderListener.add(listener);
  }
}
