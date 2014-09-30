package com.neverwinterdp.scribengin.yarn;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.neverwinterdp.scribengin.constants.Constants;
import com.neverwinterdp.scribengin.utilities.Util;

public class Client {
  // usr/lib/hadoop/bin/hadoop jar scribengin-1.0-SNAPSHOT.jar --container_mem 300 --am_mem 300 --container_cnt 1  --hdfsjar /scribengin-1.0-SNAPSHOT.jar --app_name foobar --command echo --am_class_name "com.neverwinterdp.scribengin.ScribenginAM" --topic scribe --kafka_seed_brokers 10.0.2.15:9092
  private static final Logger LOG = Logger.getLogger(Client.class.getName());
  private ApplicationId appId;
  private YarnClient yarnClient;
  private Configuration conf;

  @Parameter(names = {"-" + Constants.OPT_APPNAME, "--" + Constants.OPT_APPNAME})
  private String appname;

  @Parameter(names = {"-" + Constants.OPT_APPLICATION_MASTER_MEM,
      "--" + Constants.OPT_APPLICATION_MASTER_MEM})
  private int applicationMasterMem;
  @Parameter(names = {"-" + Constants.OPT_CONTAINER_MEM, "--" + Constants.OPT_CONTAINER_MEM})
  private int containerMem;
 
  @Parameter(names = {"-" + Constants.OPT_HDFSJAR, "--" + Constants.OPT_HDFSJAR})
  private String hdfsJar;
  
  @Parameter(names = {"-" + Constants.OPT_YARN_SITE_XML, "--" + Constants.OPT_YARN_SITE_XML})
  private String yarnSiteXml= "/etc/hadoop/conf/yarn-site.xml";

  @Parameter(names = {"-" + Constants.OPT_DEFAULT_FS, "--" + Constants.OPT_DEFAULT_FS})
  private String defaultFs="hdfs://127.0.0.1";

  
  private String applicationMasterClassName;

  @Parameter(names = {"-" + Constants.OPT_KAFKA_TOPIC, "--" + Constants.OPT_KAFKA_TOPIC},
      variableArity = true)
  private List<String> topicList;

  @Parameter(names = {"-" + Constants.OPT_KAFKA_SEED_BROKERS,
      "--" + Constants.OPT_KAFKA_SEED_BROKERS}, variableArity = true)
  private List<String> kafkaSeedBrokers;

  public Client(String appname, String hdfsJar, String applicationMasterClassName, String defaultFs, String yarnSiteXml, List<String> topicList, List<String> kafkaSeedBrokers, int containerMem, int applicationMasterMem) throws Exception{
    this();
    this.appname = appname;
    this.hdfsJar = hdfsJar;
    this.applicationMasterClassName = applicationMasterClassName; 
    this.topicList = topicList;
    this.kafkaSeedBrokers = kafkaSeedBrokers;
    this.containerMem = containerMem;
    this.applicationMasterMem = applicationMasterMem;
    this.defaultFs = defaultFs;
    this.yarnSiteXml = yarnSiteXml;
  }
  
  
  public Client() throws Exception {
    this.conf = new YarnConfiguration();
    this.yarnClient = YarnClient.createYarnClient();
    
    this.conf.addResource(new Path(this.yarnSiteXml));
    this.conf.set("fs.defaultFS", this.defaultFs);
    
    // Yarn Client's initialization determines the RM's IP address and port.
    // These values are extracted from yarn-site.xml or yarn-default.xml.
    // It also determines the interval by which it should poll for the
    // application's state.
    yarnClient.init(conf);
  }
  
  

  public void init() {
    LOG.setLevel(Level.INFO);
  }
  
  public ApplicationId run() throws IOException, YarnException {
    LOG.info("calling run.");
    yarnClient.start();

    // YarnClientApplication is used to populate:
    //   1. GetNewApplication Response
    //   2. ApplicationSubmissionContext
    YarnClientApplication app = yarnClient.createApplication();
    
    // GetNewApplicationResponse can be used to determined resources available.
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
    
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    this.appId = appContext.getApplicationId();
    appContext.setApplicationName(this.appname);

    // Set up the container launch context for AM.
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    LocalResource appMasterJar;
    FileSystem fs = FileSystem.get(this.conf);
    
    amContainer.setLocalResources(
        Collections.singletonMap("master.jar",
            Util.newYarnAppResource(fs, new Path(this.hdfsJar), LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION)));
    // Set up CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);

    // Set up resource requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(this.applicationMasterMem);
    capability.setVirtualCores(1); //TODO: Can we really setVirtualCores ?
    amContainer.setCommands(Collections.singletonList(this.getCommand()));

    // put everything together.
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue("default"); // TODO: Need to investigate more on queuing an scheduling.

    // Submit application
    yarnClient.submitApplication(appContext);
    LOG.info("APPID: "+this.appId.toString());
    return this.appId;
    //return this.monitorApplication(appId);
  }
  
  public YarnClient getYarnClient(){
    return this.yarnClient;
  }
  
  public ApplicationId getAppId(){
    return this.appId;
  }

  private String getCommand() {
    StringBuilder sb = new StringBuilder();
    sb.append(Environment.JAVA_HOME.$()).append("/bin/java").append(" ");
    sb.append("-Xmx").append(this.applicationMasterMem).append("M").append(" ");
    sb.append(this.applicationMasterClassName).append(" ");
    sb.append("--").append(Constants.OPT_CONTAINER_MEM).append(" ").append(this.containerMem)
        .append(" ");
    sb.append("--").append(Constants.OPT_KAFKA_SEED_BROKERS).append(" ")
        .append(StringUtils.join(this.kafkaSeedBrokers, " ")).append(" ");
    //sb.append("--").append(Constants.OPT_KAFKA_PORT).append(" ").append(this.port).append(" ");
    sb.append("--").append(Constants.OPT_KAFKA_TOPIC).append(" ")
        .append(StringUtils.join(this.topicList, " ")).append(" ");

    sb.append("1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout")
        .append(" ");
    sb.append("2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr");
    String r = sb.toString();
    LOG.info("ApplicationConstants.getCommand() : " + r);
    return r;
  }

  private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
    boolean r = false;
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        r = false;
        break;
      }

      ApplicationReport report = yarnClient.getApplicationReport(appId);
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus status = report.getFinalApplicationStatus();

      if (state == YarnApplicationState.FINISHED) {
        if (status == FinalApplicationStatus.SUCCEEDED) {
          LOG.info("Completed sucessfully.");
          r = true;
          break;
        } else {
          LOG.info("Application errored out. YarnState=" + state.toString() + ", finalStatue="
              + status.toString());
          r = false;
          break;
        }
      } else if (state == YarnApplicationState.KILLED || state == YarnApplicationState.FAILED) {
        LOG.info("Application errored out. YarnState=" + state.toString() + ", finalStatue="
            + status.toString());
        r = false;
        break;
      }
    }// while
    return r;
  }

  private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
    StringBuilder classPathEnv = new StringBuilder();
    classPathEnv.append(Environment.CLASSPATH.$()).append(File.pathSeparatorChar);
    classPathEnv.append("./*");

    for (String c : conf.getStrings(
        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(File.pathSeparatorChar);
      classPathEnv.append(c.trim());
    }

    String envStr = classPathEnv.toString();
    LOG.info("env: " + envStr);
    appMasterEnv.put(Environment.CLASSPATH.name(), envStr);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("main");
    Client c = new Client();
    boolean r = false;

    JCommander jc = new JCommander(c);
    jc.parse(args);
    
    c.init();
    ApplicationId appId = c.run();
    r = c.monitorApplication(appId);
    if (r) {
      System.exit(0);
    }
    System.exit(2);
  }
}
