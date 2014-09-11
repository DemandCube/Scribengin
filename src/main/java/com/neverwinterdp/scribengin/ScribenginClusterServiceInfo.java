package com.neverwinterdp.scribengin;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.server.service.ServiceInfo;
/**
 * @author Richard Duarte
 */
public class ScribenginClusterServiceInfo extends ServiceInfo{
  @Inject @Named("scribengin:checkpointinterval")
  private String checkpointinterval = "0";
  
  @Inject @Named("scribengin:leader")
  private String leader = "localhost:9091";
  
  @Inject @Named("scribengin:partition")
  private String partition = "0";
  
  @Inject @Named("scribengin:topic")
  private String topic = "scribe";
  
  
  public String getCheckpointInterval(){return checkpointinterval; }
  public String getLeader(){return leader; }
  public String getPartition(){return partition; }
  public String getTopic(){return topic; }
  
}