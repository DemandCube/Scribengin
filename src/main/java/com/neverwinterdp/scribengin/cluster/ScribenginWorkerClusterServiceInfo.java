package com.neverwinterdp.scribengin.cluster;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.neverwinterdp.server.service.ServiceInfo;
/**
 * @author Richard Duarte
 */
public class ScribenginWorkerClusterServiceInfo extends ServiceInfo{
  @Inject @Named("scribenginworker:checkpointinterval")
  private long checkpointinterval = 0;
  
  @Inject @Named("scribenginworker:leaderHost")
  private String leaderHost = "localhost";
  
  @Inject @Named("scribenginworker:leaderPort")
  private int leaderPort = 9091;
  
  @Inject @Named("scribenginworker:partition")
  private int partition = 0;
  
  @Inject @Named("scribenginworker:topic")
  private String topic = "scribe";
  
  @Inject @Named("scribenginworker:hdfsPath")
  private String hdfsPath = "";
  
  @Inject @Named("scribenginworker:preCommitPathPrefix")
  private String preCommitPathPrefix = "/tmp";
  
  @Inject @Named("scribenginworker:commitPathPrefix")
  private String commitPathPrefix = "/committed";
  
  public long getCheckpointInterval(){return checkpointinterval; }
  public String getLeaderHost(){return leaderHost; }
  public int getLeaderPort(){return leaderPort; }
  public int getPartition(){return partition; }
  public String getTopic(){return topic; }
  public String getHdfsPath(){return hdfsPath; }
  public String getPreCommitPathPrefix(){return preCommitPathPrefix; }
  public String getCommitPathPrefix(){return commitPathPrefix; }
  
  
}