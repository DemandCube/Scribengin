package com.neverwinterdp.scribengin.cluster;

import org.slf4j.Logger;

import com.google.inject.Injector;
import com.google.inject.Inject;
import com.neverwinterdp.scribengin.ScribeMaster;
import com.neverwinterdp.scribengin.ScribeConsumerManager.ClusterScribeConsumerManager;
import com.neverwinterdp.scribengin.ScribeConsumerManager.YarnScribeConsumerManager;
import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;
import com.neverwinterdp.server.service.AbstractService;
import com.neverwinterdp.server.module.ModuleProperties;
import com.neverwinterdp.util.LoggerFactory;

public class ScribeMasterClusterService extends AbstractService{
  private Logger logger ;
  private ScribeMaster sm;
  private ScribeMasterClusterServiceInfo serviceInfo;
  
  @Inject
  public void init(Injector container,
                   LoggerFactory factory, 
                   ModuleProperties moduleProperties,
                   ScribeMasterClusterServiceInfo serviceInfo) throws Exception {
    this.logger = factory.getLogger(ScribeMasterClusterService.class) ;
    this.serviceInfo = serviceInfo ;
  }
  
  
  public void start() throws Exception {
    logger.info("Starting ScribeMaster");
    ScribeConsumerConfig conf = new ScribeConsumerConfig();
    
    conf.applicationMasterMem = this.serviceInfo.applicationMasterMem;
    conf.appMasterClassName = this.serviceInfo.appMasterClassName;
    conf.appname = this.serviceInfo.appname;
    conf.brokerList = this.serviceInfo.getKafkaAsList();
    conf.cleanStart = this.serviceInfo.cleanstart;
    conf.COMMIT_PATH_PREFIX = this.serviceInfo.commitPrefix;
    conf.commitCheckPointInterval = this.serviceInfo.commitCheckPointInterval;
    conf.containerMem = this.serviceInfo.containerMem;
    conf.hdfsPath = this.serviceInfo.hdfsPath;
    conf.partition = this.serviceInfo.partition;
    conf.PRE_COMMIT_PATH_PREFIX = this.serviceInfo.preCommitPrefix;
    conf.scribenginJarPath = this.serviceInfo.scribenginjar;
    conf.topic = null;
    
    sm = new ScribeMaster(this.serviceInfo.getTopicsAsList(), conf);
    
    if(this.serviceInfo.mode != "yarn"){
      sm.setScribeConsumerManager(new ClusterScribeConsumerManager());
    }
    else{
      sm.setScribeConsumerManager(new YarnScribeConsumerManager());
    }
    
    sm.start();
    logger.info("Starting ScribeMaster Complete");
    
  }

  public void stop() {
    logger.info("Stopping ScribeMaster");
    sm.stop();
    logger.info("Stopping ScribeMaster Complete");
  }
}
