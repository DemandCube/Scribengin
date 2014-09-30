package com.neverwinterdp.scribengin.cluster;

import java.util.LinkedList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.scribengin.constants.Constants;
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.neverwinterdp.server.service.ServiceInfo;

public class ScribeMasterClusterServiceInfo extends ServiceInfo {
  @Inject @Named("scribemaster:topics")
  private String topics;
  
  @Inject @Named("scribemaster:preCommitPrefix")
  public String preCommitPrefix="/tmp";

  @Inject @Named("scribemaster:commitPrefix")
  public String commitPrefix="/committed";

  @Inject @Named("scribemaster:hdfsPath")
  public String hdfsPath = null;

  @Inject @Named("scribemaster:partition")
  public int partition;

  @Inject @Named("scribemaster:brokerList")
  public List<HostPort> brokerList; // list of (host:port)s

  @Inject @Named("scribemaster:commitCheckPointInterval")
  public long commitCheckPointInterval; // ms
  
  @Inject @Named("scribemaster:cleanStart")
  public boolean cleanstart = false;
  
  ////////////////////////
  //Yarn config parameters
  @Inject @Named("scribemaster:appname")
  public String appname = "ScribeConsumer";
  
  @Inject @Named("scribemaster:scribenginJar")
  public String scribenginjar = "/scribengin-1.0-SNAPSHOT.jar";
  
  @Inject @Named("scribemaster:appMasterClassName")
  public String appMasterClassName = com.neverwinterdp.scribengin.yarn.ScribenginAM.class.getName();
  
  @Inject @Named("scribemaster:containerMem")
  public int containerMem = 1024;
  
  @Inject @Named("scribemaster:applicationMasterMem")
  public int applicationMasterMem = 300;
  
  public String getTopics(){ return topics;}
  public List<String> getTopicsAsList(){
    List<String> ret = new LinkedList<String>();
    String[] split = this.topics.split(",");
    for(String x: split){
      ret.add(x);
    }
    return ret;
  }
}
