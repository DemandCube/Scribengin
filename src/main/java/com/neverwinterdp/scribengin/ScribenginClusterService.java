package com.neverwinterdp.scribengin;

import java.io.IOException;

import org.slf4j.Logger;

import com.google.inject.Injector;
import com.google.inject.Inject;
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
  Thread t;
  @Inject
  public void init(Injector container,
                   LoggerFactory factory, 
                   ModuleProperties moduleProperties,
                   ScribenginClusterServiceInfo serviceInfo) throws Exception {
    //this.loggerFactory = factory ;
    this.logger = factory.getLogger(ScribenginClusterService.class) ;
    this.serviceInfo = serviceInfo ;
  }
  
  
  public void start() throws Exception {
    logger.info("Starting Scribengin");
    final String[] args = {"--topic", this.serviceInfo.getTopic(), 
                      "--leader", this.serviceInfo.getLeader(),
                      "--checkpoint_interval", this.serviceInfo.getCheckpointInterval(),
                      "--partition", this.serviceInfo.getPartition(),
                    };
    t = new Thread(){
      public void run(){
        try {
          //TODO: ScribeConsumer should be able to run as a thread on its own
          ScribeConsumer.main(args);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    };
    t.start();
    logger.info("Starting Scribengin Complete");
    
  }

  public void stop() {
    logger.info("Stopping Scribengin");
    t.interrupt();
    logger.info("Stopping Scribengin Complete");
  }
}
