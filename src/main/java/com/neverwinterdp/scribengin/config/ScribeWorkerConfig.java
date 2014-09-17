package com.neverwinterdp.scribengin.config;

import org.apache.log4j.Logger;

public class ScribeWorkerConfig {
  private static final Logger log = Logger.getLogger(ScribeWorkerConfig.class.getName());
  
  public String PRE_COMMIT_PATH_PREFIX;
  public String COMMIT_PATH_PREFIX;
  public String topic;
  public String leaderHost;
  public int leaderPort;
  public int partition;
  public String hdfsPath;
  public long commitCheckPointInterval; // ms

  public ScribeWorkerConfig(String leaderHost,
                           int leaderPort,
                           String topic,
                           String preCommitPathPrefix,
                           String commitPathPrefix,
                           int partition,
                           String hdfsPath, 
                           long commitCheckPointInterval){
    
    this.PRE_COMMIT_PATH_PREFIX = preCommitPathPrefix;
    this.COMMIT_PATH_PREFIX = commitPathPrefix;
    this.topic = topic;
    this.leaderHost = leaderHost;
    this.leaderPort = leaderPort;
    this.partition = partition;
    this.commitCheckPointInterval = commitCheckPointInterval; // ms
    this.hdfsPath = hdfsPath;
}

  public ScribeWorkerConfig(String leaderHost, int leaderPort,String topic){
    this(leaderHost, leaderPort, topic, "/tmp","/committed", 0, null, 100);
  }
}
