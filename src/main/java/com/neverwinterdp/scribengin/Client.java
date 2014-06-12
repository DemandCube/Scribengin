package com.neverwinterdp.scribengin;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class Client {
  private static final Logger LOG = Logger.getLogger(Client.class.getName());
  private YarnClient yarnClient;
  private Configuration conf;

  @Parameter(names = {"-" + Constants.OPT_APPNAME, "--" + Constants.OPT_APPNAME})
  private String appname;
  @Parameter(names = {"-" + Constants.OPT_COMMAND, "--" + Constants.OPT_COMMAND})
  private String command;
  @Parameter(names = {"-" + Constants.OPT_APPLICATION_MASTER_MEM, "--" + Constants.OPT_APPLICATION_MASTER_MEM})
  private int applicationMasterMem;
  @Parameter(names = {"-" + Constants.OPT_CONTAINER_MEM, "--" + Constants.OPT_CONTAINER_MEM})
  private int containerMem;
  @Parameter(names = {"-" + Constants.OPT_CONTAINER_COUNT, "--" + Constants.OPT_CONTAINER_COUNT})
  private int containerCount;

  @Parameter(names = {"-" + Constants.OPT_HDFSJAR, "--" + Constants.OPT_HDFSJAR})
  private String hdfsJar;
  @Parameter(names = {"-" + Constants.OPT_APPLICATION_MASTER_CLASS_NAME, "--" + Constants.OPT_APPLICATION_MASTER_CLASS_NAME})
  private String applicationMasterClassName;

  public Client() throws Exception{
    this.conf = new YarnConfiguration();
    this.yarnClient = YarnClient.createYarnClient();
    //
    // Yarn Client's initialization determines the RM's IP address and port.
    // These values are extracted from yarn-site.xml or yarn-default.xml.
    // It also determines the interval by which it should poll for the
    // application's state.
    yarnClient.init(conf);
  }

  public void init(String[] args) {
    LOG.setLevel(Level.INFO);
    LOG.info("command: " + this.command);
  }

  public boolean run() throws IOException, YarnException {
    LOG.info("calling run.");
    yarnClient.start();

    // YarnClientApplication is used to populate:
    //   1. GetNewApplication Response
    //   2. ApplicationSubmissionContext
    YarnClientApplication app = yarnClient.createApplication();

    // GetNewApplicationResponse can be used to determined resources available.
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    appContext.setApplicationName(this.appname);

    // Set up the container launch context for AM.
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    LocalResource appMasterJar;
    appMasterJar = this.setupAppMasterJar(this.hdfsJar);
    amContainer.setLocalResources(
        Collections.singletonMap("ae_master.jar", appMasterJar));

    // Set up CLASSPATH for ApplicationMaster
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv);
    amContainer.setEnvironment(appMasterEnv);

    // Set up resource requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(this.applicationMasterMem);
    capability.setVirtualCores(1);  //TODO: Can we really setVirtualCores ?
    amContainer.setCommands(Collections.singletonList(this.getCommand()));

    // put everything together.
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue("default"); // TODO: Need to investigate more on queuing an scheduling.

    // Submit application
    yarnClient.submitApplication(appContext);

    return this.monitorApplication(appId);
  }

  private String getCommand() {
    StringBuilder sb = new StringBuilder();
    sb.append(Environment.JAVA_HOME.$()).append("/bin/java").append(" ");
    sb.append("-Xmx").append(this.applicationMasterMem).append("M").append(" ");
    sb.append(this.applicationMasterClassName).append(" ");
    sb.append("--").append(Constants.OPT_CONTAINER_MEM).append(" ").append(this.containerMem).append(" ");
    sb.append("--").append(Constants.OPT_CONTAINER_COUNT).append(" ").append(this.containerCount).append(" ");
    sb.append("--").append(Constants.OPT_COMMAND).append(" '").append(StringEscapeUtils.escapeJava(this.command)).append("' ");

    sb.append("1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout").append(" ");
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
          LOG.info("Application errored out. YarnState=" + state.toString() + ", finalStatue=" + status.toString());
          r = false;
          break;
        }
      } else if (state == YarnApplicationState.KILLED || state == YarnApplicationState.FAILED) {
        LOG.info("Application errored out. YarnState=" + state.toString() + ", finalStatue=" + status.toString());
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


  private LocalResource setupAppMasterJar(FileStatus status, Path jarHdfsPath) throws IOException {
    LocalResource appMasterJar =  Records.newRecord(LocalResource.class);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarHdfsPath));
    appMasterJar.setSize(status.getLen());
    appMasterJar.setTimestamp(status.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
    return appMasterJar;
  }

  // Assume that the hdfsPath already exits in HDFS
  private LocalResource setupAppMasterJar(String hdfsPath) throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    Path dst = new Path(hdfsPath);
    dst = fs.makeQualified(dst); // must use fully qualified path name. Otherise, nodemanager gets angry.
    return this.setupAppMasterJar(fs.getFileStatus(dst), dst);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("main");
    Client c = new Client();
    boolean r = false;

    new JCommander(c, args);
    c.init(args);
    r = c.run();

    if (r) {
      System.exit(0);
    }
    System.exit(2);
  }
}


