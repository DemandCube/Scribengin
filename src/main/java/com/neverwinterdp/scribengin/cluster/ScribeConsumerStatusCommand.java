package com.neverwinterdp.scribengin.cluster;

import com.neverwinterdp.server.Server;
import com.neverwinterdp.server.command.ServiceCommand;
import com.neverwinterdp.server.service.Service;

public class ScribeConsumerStatusCommand extends ServiceCommand<Thread.State> {

  @Override
  public Thread.State execute(Server server, Service service) throws Exception {
    ScribeConsumerClusterService s = (ScribeConsumerClusterService) service;
    return s.getServiceState();
  }

}
