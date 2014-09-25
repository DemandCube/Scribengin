package com.neverwinterdp.scribengin;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.constants.Constants;
import com.neverwinterdp.scribengin.hostport.HostPort;

class CommandLineArgs {
  @Parameter(names = {"-"+Constants.OPT_PRE_COMMIT_PATH_PREFIX, "--"+Constants.OPT_PRE_COMMIT_PATH_PREFIX}, description="Pre commit path")
  public String preCommitPrefix="/tmp";
  
  @Parameter(names = {"-"+Constants.OPT_COMMIT_PATH_PREFIX, "--"+Constants.OPT_COMMIT_PATH_PREFIX}, description="Commit path")
  public String commitPrefix="/committed";
  
  @Parameter(names = {"-"+Constants.OPT_KAFKA_TOPIC, "--"+Constants.OPT_KAFKA_TOPIC}, required = true, description="Kafka topic to read from")
  public String topic;

  @Parameter(names = {"-"+Constants.OPT_HDFS_PATH, "--"+Constants.OPT_HDFS_PATH}, description="Host:Port of HDFS path")
  public String hdfsPath = null;

  @Parameter(names = {"-"+Constants.OPT_PARTITION, "--"+Constants.OPT_PARTITION}, required = true, description="Kafka partition")
  public int partition;

  @Parameter(names = {"-"+Constants.OPT_BROKER_LIST, "--"+Constants.OPT_BROKER_LIST}, variableArity = true, required = true, description="List of Kafka's in host:port format")
  public List<HostPort> brokerList; // list of (host:port)s

  @Parameter(names = {"-"+Constants.OPT_CHECK_POINT_TIMER, "--"+Constants.OPT_CHECK_POINT_TIMER}, description="Check point interval in milliseconds", required = true)
  public long commitCheckPointInterval; // ms
  
  @Parameter(names={"-"+Constants.OPT_CLEAN_START,"--"+Constants.OPT_CLEAN_START}, description="If enabled, sets start offset to 0")
  public boolean cleanstart = false;
  
  @Parameter(names={"-help","--help","-h"}, description="Displays help message")
  public boolean help = false;
}