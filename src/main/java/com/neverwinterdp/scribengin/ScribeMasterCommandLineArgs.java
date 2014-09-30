package com.neverwinterdp.scribengin;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.constants.Constants;
import com.neverwinterdp.scribengin.hostport.HostPort;
import com.beust.jcommander.converters.CommaParameterSplitter;

public class ScribeMasterCommandLineArgs {
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

  @Parameter(names = {"-"+Constants.OPT_CHECK_POINT_TIMER, "--"+Constants.OPT_CHECK_POINT_TIMER}, description="Check point interval in milliseconds")
  public long commitCheckPointInterval; // ms
  
  @Parameter(names={"-"+Constants.OPT_CLEAN_START,"--"+Constants.OPT_CLEAN_START}, description="If enabled, sets start offset to 0")
  public boolean cleanstart = false;
  
  @Parameter(names={"-help","--help","-h"}, description="Displays help message")
  public boolean help = false;
  
  
  ////////////////////////
  //Yarn config parameters
  @Parameter(names={"-"+Constants.OPT_YARN_APPNAME,"--"+Constants.OPT_YARN_APPNAME,}, description="Application name for Yarn mode")
  public String appname = "ScribeConsumer";
  
  @Parameter(names={"-"+Constants.OPT_SCRIBENGIN_JAR,"--"+Constants.OPT_SCRIBENGIN_JAR,}, description="Path to scribengin jar on HDFS - ex: /scribengin-1.0-SNAPSHOT.jar")
  public String scribenginjar = "/scribengin-1.0-SNAPSHOT.jar";
  
  @Parameter(names={"-"+Constants.OPT_APP_MASTER_CLASS_NAME,"--"+Constants.OPT_APP_MASTER_CLASS_NAME,}, description="Application Master Class Name")
  public String appMasterClassName = com.neverwinterdp.scribengin.yarn.ScribenginAM.class.getName();
  
  @Parameter(names={"-"+Constants.OPT_CONTAINER_MEMORY,"--"+Constants.OPT_CONTAINER_MEMORY,}, description="Memory to allocate for container")
  public int containerMem = 1024;
  
  @Parameter(names={"-"+Constants.OPT_APP_MASTER_MEMORY,"--"+Constants.OPT_APP_MASTER_MEMORY,}, description="Memory to allocate for Application Master")
  public int applicationMasterMem = 300;
  
  @Parameter(names={"-"+Constants.OPT_MODE, "--"+Constants.OPT_MODE}, description="Mode to run in. Valid options are dev, distributed, and yarn")
  public String mode = "dev";
}
