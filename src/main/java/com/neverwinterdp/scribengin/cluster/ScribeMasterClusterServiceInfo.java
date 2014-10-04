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
  
  @Inject(optional=true) @Named("scribemaster:preCommitPrefix")
  public String preCommitPrefix="/tmp";

  @Inject(optional=true) @Named("scribemaster:commitPrefix")
  public String commitPrefix="/committed";

  @Inject(optional=true) @Named("scribemaster:hdfsPath")
  public String hdfsPath = null;

  @Inject(optional=true) @Named("scribemaster:partition")
  public int partition=0;

  @Inject @Named("scribemaster:brokerList")
  public String brokerList; // list of (host:port)s

  @Inject(optional=true) @Named("scribemaster:commitCheckPointInterval")
  public long commitCheckPointInterval=500; // ms
  
  @Inject(optional=true) @Named("scribemaster:cleanStart")
  public boolean cleanstart = false;
  
  
  
  ////////////////////////
  //Yarn config parameters
  @Inject(optional=true) @Named("scribemaster:appName")
  public String appname = "ScribeConsumer";
  
  @Inject(optional=true) @Named("scribemaster:scribenginJar")
  public String scribenginjar = "/scribengin-1.0-SNAPSHOT.jar";
  
  @Inject(optional=true) @Named("scribemaster:appMasterClassName")
  public String appMasterClassName = com.neverwinterdp.scribengin.yarn.ScribenginAM.class.getName();
  
  @Inject(optional=true) @Named("scribemaster:containerMem")
  public int containerMem = 1024;
  
  @Inject(optional=true) @Named("scribemaster:applicationMasterMem")
  public int applicationMasterMem = 300;
  
  @Inject(optional=true) @Named("scribemaster:mode")
  public String mode = "distributed";
  
  public String getTopics(){ return topics;}
  public List<String> getTopicsAsList(){
    List<String> ret = new LinkedList<String>();
    String[] split = this.topics.split(",");
    for(String x: split){
      ret.add(x);
    }
    return ret;
  }
  
  public List<HostPort> getKafkaAsList(){
    List<HostPort> res = new LinkedList<HostPort>();
    String[] splitBrokers = this.brokerList.split(",");
    for(String x: splitBrokers){
      String[] split = x.split(":");
      res.add(new HostPort(split[0],split[1]));
    }
    return res;
  }
}
