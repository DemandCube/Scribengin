package com.neverwinterdp.scribengin;

import org.slf4j.Logger;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.neverwinterdp.server.module.ModuleProperties;
import com.neverwinterdp.server.service.AbstractService;
import com.neverwinterdp.util.LoggerFactory;

public class ScribenginClusterService extends AbstractService {
  //private LoggerFactory loggerFactory ;
  private Logger logger ;
  private ScribenginClusterServiceInfo serviceInfo;
  private Scribengin server;
  
  @Inject
  public void init(Injector container,
                   LoggerFactory factory, 
                   ModuleProperties moduleProperties,
                   ScribenginClusterServiceInfo serviceInfo) throws Exception {
    //this.loggerFactory = factory ;
    this.logger = factory.getLogger(ScribenginClusterService.class) ;
    this.serviceInfo = serviceInfo ;
  }
  
  
  public void stop() {
    logger.info("Stopping Scribengin");
    server.stop();
    logger.info("Stopping Scribengin complete");
  }
  

  @Override
  public void start() throws Exception {
    logger.info("Starting Scribengin");
    server = new Scribengin(this.serviceInfo.getServerPropertyFile());
    server.init();
    server.start();
    logger.info("Starting Scribengin complete");
  }
  
}