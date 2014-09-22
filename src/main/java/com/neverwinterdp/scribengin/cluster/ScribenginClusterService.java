package com.neverwinterdp.scribengin.cluster;

import org.slf4j.Logger;

import com.google.inject.Injector;
import com.google.inject.Inject;

import com.neverwinterdp.scribengin.scribengin.Scribengin;
import com.neverwinterdp.server.service.AbstractService;
import com.neverwinterdp.server.module.ModuleProperties;
import com.neverwinterdp.util.LoggerFactory;

/**
 * @author Richard Duarte
 */
public class ScribenginClusterService extends AbstractService {
  //private ScribeConsumer sc;
  private Logger logger ;
  private ScribenginClusterServiceInfo serviceInfo;
  private Scribengin s;
  
  @Inject
  public void init(Injector container,
                   LoggerFactory factory, 
                   ModuleProperties moduleProperties,
                   ScribenginClusterServiceInfo serviceInfo) throws Exception {
    //this.loggerFactory = factory ;
    this.logger = factory.getLogger(ScribenginWorkerClusterService.class) ;
    this.serviceInfo = serviceInfo ;
  }
  
  
  public void start() throws Exception {
    logger.info("Starting Scribengin");
    s = new Scribengin(this.serviceInfo.getTopics(), this.serviceInfo.getKafkaHost(), this.serviceInfo.getKafkaPort(), this.serviceInfo.getWorkerCheckTimerInterval());
    s.start();
    logger.info("Starting Scribengin Complete");
  }

  public void stop() {
    logger.info("Stopping Scribengin");
    s.stop();
    logger.info("Stopping Scribengin Complete");
  }
}
