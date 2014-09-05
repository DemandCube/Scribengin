package com.neverwinterdp.scribengin;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.server.module.ModuleProperties;
import com.neverwinterdp.server.service.AbstractService;
import com.neverwinterdp.util.LoggerFactory;

public class ScribenginClusterService extends AbstractService {
  private LoggerFactory loggerFactory ;
  private Logger logger ;
  private ScribenginClusterServiceInfo serviceInfo;
  private Scribengin server;
  
  @Inject
  public void init(Injector container,
                   LoggerFactory factory, 
                   ModuleProperties moduleProperties,
                   ScribenginClusterServiceInfo serviceInfo) throws Exception {
    
    this.loggerFactory = factory ;
    logger = factory.getLogger(ScribenginClusterService.class) ;
    this.serviceInfo = serviceInfo ;
    if(moduleProperties.isDataDrop()) cleanup() ;
  }
  
  
  public void stop() {
    //Need a way to kill the server thread
  }
  
  /**
   * TODO: serviceInfo is returning null at the moment, need to fix injection
   */
  @Override
  public void start() throws Exception {
    server = new Scribengin(this.serviceInfo.getExample());
    
    //main() needs to start as a thread or as a daemon
    //server.main(null);
  }
  
}