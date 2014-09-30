package com.neverwinterdp.scribengin.ScribeConsumerManager;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;

import com.neverwinterdp.scribengin.scribeconsumer.ScribeConsumerConfig;
import com.neverwinterdp.scribengin.yarn.Client;

public class YarnScribeConsumerManager extends AbstractScribeConsumerManager{
  private class YarnInfo{
    @SuppressWarnings("unused")
    public Client client;
    @SuppressWarnings("unused")
    public ScribeConsumerConfig conf;
    
    public YarnInfo(Client c, ScribeConsumerConfig cnf){
      this.client = c;
      this.conf = cnf;
    }
  }
  
  private static final Logger LOG = Logger.getLogger(YarnScribeConsumerManager.class.getName());
  List<YarnInfo> yarnApps = new LinkedList<YarnInfo>();
  
  
  public YarnScribeConsumerManager(){
    super();
  }
  
  @Override
  public boolean startNewConsumer(ScribeConsumerConfig conf) {
    Client client = null;
    try {
      List<String> topics = new LinkedList<String>();
      topics.add(conf.topic);
      client = new Client(conf.appname, 
                          conf.scribenginjar, 
                          conf.appMasterClassName, 
                          conf.yarnSiteXml, 
                          conf.defaultFs, 
                          topics,
                          conf.getBrokerListAsListOfStrings(), 
                          conf.containerMem, 
                          conf.applicationMasterMem
                        );
    } catch (Exception e) {
      e.printStackTrace();
    }
    try {
      client.init();
      client.run();
      yarnApps.add(new YarnInfo(client, conf));
      return true;
    } catch (IOException | YarnException e) {
      e.printStackTrace();
    }
    return false;
  }

  
  @Override 
  public void monitorConsumers(){
    Iterator<YarnInfo> it = yarnApps.iterator();
    
    while (it.hasNext()) {
      YarnInfo yarnInfo = it.next();
      ApplicationReport report = null;
      Client c = null;
      try {
        c = yarnInfo.client;
        report = c.getYarnClient().getApplicationReport(c.getAppId());
      } catch (YarnException | IOException e) {
        e.printStackTrace();
      };
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus status = report.getFinalApplicationStatus();

      if (state == YarnApplicationState.FINISHED) {
        if (status == FinalApplicationStatus.SUCCEEDED) {
          LOG.info("Application completed successfully: "+c.getAppId().toString());
          it.remove();
          break;
        } else {
          LOG.info("Application finished but errored out. YarnState=" + state.toString() + ", finalStatue=" + status.toString() + ", AppId: "+c.getAppId().toString());
          if(startNewConsumer(yarnInfo.conf)){
            it.remove();
          }
          break;
        }
      } else if (state == YarnApplicationState.KILLED || state == YarnApplicationState.FAILED) {
        LOG.info("Application errored out. YarnState=" + state.toString() + ", finalStatue=" + status.toString() + ", AppId: "+c.getAppId().toString());
        it.remove();
        break;
      }
    }
  }

  @Override
  public boolean shutdownConsumers() {
    boolean retVal = true;
    Iterator<YarnInfo> it = yarnApps.iterator();
    while(it.hasNext()){
      YarnInfo yi = it.next();
      try{
        yi.client.getYarnClient().killApplication(yi.client.getAppId());
        it.remove();
      } catch(Exception e){
        e.printStackTrace();
        retVal = false;
      }
    }
    return retVal;
  }
}
