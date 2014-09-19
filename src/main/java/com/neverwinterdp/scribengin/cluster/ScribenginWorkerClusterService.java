package com.neverwinterdp.scribengin.cluster;

import org.slf4j.Logger;

import com.google.inject.Injector;
import com.google.inject.Inject;
import com.neverwinterdp.scribengin.scribeworker.ScribeWorker;
import com.neverwinterdp.scribengin.scribeworker.config.ScribeWorkerConfig;
import com.neverwinterdp.server.service.AbstractService;
import com.neverwinterdp.server.module.ModuleProperties;
import com.neverwinterdp.util.LoggerFactory;

/**
 * @author Richard Duarte
 */
public class ScribenginWorkerClusterService extends AbstractService {
  //private ScribeConsumer sc;
  private Logger logger ;
  private ScribenginWorkerClusterServiceInfo serviceInfo;
  private ScribeWorkerConfig s;
  private ScribeWorker sw;
  
  @Inject
  public void init(Injector container,
                   LoggerFactory factory, 
                   ModuleProperties moduleProperties,
                   ScribenginWorkerClusterServiceInfo serviceInfo) throws Exception {
    //this.loggerFactory = factory ;
    this.logger = factory.getLogger(ScribenginWorkerClusterService.class) ;
    this.serviceInfo = serviceInfo ;
  }
  
  
  public void start() throws Exception {
    logger.info("Starting ScribenginWorker");
    s = new ScribeWorkerConfig(this.serviceInfo.getLeaderHost(), 
                         this.serviceInfo.getLeaderPort(),
                         this.serviceInfo.getTopic(),
                         this.serviceInfo.getPreCommitPathPrefix(),
                         this.serviceInfo.getCommitPathPrefix(),
                         this.serviceInfo.getPartition(),
                         this.serviceInfo.getHdfsPath(),
                         this.serviceInfo.getCheckpointInterval());
    sw = new ScribeWorker(s);
    sw.start();
    logger.info("Starting ScribenginWorker Complete");
    
  }

  public void stop() {
    logger.info("Stopping ScribenginWorker");
    sw.stop();
    logger.info("Stopping ScribenginWorker Complete");
  }
}
