package com.neverwinterdp.scribengin.cluster;

import java.util.LinkedList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.neverwinterdp.server.service.ServiceInfo;

public class ScribeConsumerClusterServiceInfo extends ServiceInfo{
  @Inject @Named("scribeconsumer:precommitpathprefix")
  private String PRE_COMMIT_PATH_PREFIX;
  
  @Inject @Named("scribeconsumer:commitpathprefix")
  private String COMMIT_PATH_PREFIX;
  
  @Inject @Named("scribeconsumer:topic")
  private String topic;
  
  @Inject @Named("scribeconsumer:partition")
  private int partition;
  
  @Inject @Named("scribeconsumer:brokerList")
  private String brokerList; // list of (host:port)s
  
  @Inject @Named("scribeconsumer:commitCheckPointInterval")
  private long commitCheckPointInterval; // ms
  
  @Inject(optional=true) @Named("scribeconsumer:hdfsPath")
  private String hdfsPath = null;
  
  //Set by cleanStart() method
  @Inject @Named("scribeconsumer:cleanStart")
  private boolean cleanStart = false;
  
  
  public String getPreCommitPathPrefix(){ return this.PRE_COMMIT_PATH_PREFIX; }
  public String getCommitPathPrefix(){ return this.COMMIT_PATH_PREFIX;}
  public String getTopic(){ return this.topic;}
  public int getPartition(){ return this.partition;}
  public long getCommitCheckPointInterval(){ return this.commitCheckPointInterval;}
  public String getHdfsPath(){ return this.hdfsPath;}
  public boolean getCleanStart(){ return this.cleanStart;}
  
  public List<HostPort> getBrokerList(){
    List<HostPort> bl = new LinkedList<HostPort>();
    String[] split = this.brokerList.split(",");
    
    if(split.length < 1){
      System.err.println("Improperly formatted broker list: "+ this.brokerList);
      return null;
    }
    
    for(String hostport: split){
      String[] x = hostport.split(":");
      if(x.length != 2){
        System.err.println("Improperly formatted broker: "+ hostport);
        return null;
      }
      bl.add(new HostPort(x[0],x[1]));
    }
    return bl;
  }
}
