package com.neverwinterdp.scribengin.cluster;

import java.util.List;

import org.slf4j.Logger;

import com.google.inject.Injector;
import com.google.inject.Inject;
import com.neverwinterdp.scribengin.ScribeConsumer;
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.neverwinterdp.server.service.AbstractService;
import com.neverwinterdp.server.module.ModuleProperties;
import com.neverwinterdp.util.LoggerFactory;

public class ScribeConsumerClusterService extends AbstractService {
  private Logger logger ;
  private ScribeConsumer sc;
  private ScribeConsumerClusterServiceInfo serviceInfo;
  
  @Inject
  public void init(Injector container,
                   LoggerFactory factory, 
                   ModuleProperties moduleProperties,
                   ScribeConsumerClusterServiceInfo serviceInfo) throws Exception {
    this.logger = factory.getLogger(ScribeConsumerClusterService.class) ;
    this.serviceInfo = serviceInfo ;
  }
  
  
  public void start() throws Exception {
    logger.info("Starting ScribeConsumer");
    sc = new ScribeConsumer(this.serviceInfo.getPreCommitPathPrefix(), 
                            this.serviceInfo.getCommitPathPrefix(),
                            this.serviceInfo.getTopic(),
                            this.serviceInfo.getPartition(),
                            this.serviceInfo.getBrokerList(),
                            this.serviceInfo.getCommitCheckPointInterval(),
                            this.serviceInfo.getHdfsPath());
    sc.init();
    sc.start();
    logger.info("Starting ScribeConsumer Complete");
    
  }

  public void stop() {
    logger.info("Stopping ScribeConsumer");
    sc.stop();
    logger.info("Stopping ScribeConsumer Complete");
  }
}
