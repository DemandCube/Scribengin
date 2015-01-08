package com.neverwinterdp.vm.yarn;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.hadoop.yarn.app.Util;
import com.neverwinterdp.vm.VMConfig;

public class AppClient  {
  
  public void run(String[] args) throws Exception {
    run(args, new YarnConfiguration()) ;
  }
  
  public void run(String[] args, Configuration conf) throws Exception {
    try {
      VMConfig vmConfig = new VMConfig() ;
      new JCommander(vmConfig, args) ;
      vmConfig.overrideYarnConfiguration(conf);
      
      uploadApp(vmConfig);
      
      System.out.println("Create YarnClient") ;
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(conf);
      yarnClient.start();

      System.out.println("Create YarnClientApplication via YarnClient") ;
      YarnClientApplication app = yarnClient.createApplication();
      String appId =  app.getApplicationSubmissionContext().getApplicationId().toString() ;
      System.out.println("Application Id = " + appId) ;
      System.out.println("Set up the container launch context for the application master") ;
      ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
      StringBuilder sb = new StringBuilder();
      List<String> commands = Collections.singletonList(
          sb.append(vmConfig.buildCommand()).
          append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout").
          append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr")
          .toString()
      );
      amContainer.setCommands(commands) ;

      System.out.println("Setup the app classpath and resources") ;
      if(vmConfig.getDfsHome() != null) {
        amContainer.setLocalResources(createLocalResources(conf, vmConfig));
      }
      
      
      System.out.println("Setup the classpath for ApplicationMaster") ;
      Map<String, String> appMasterEnv = new HashMap<String, String>();
      Util.setupAppMasterEnv(vmConfig.isMiniClusterEnv(), conf, appMasterEnv);
      amContainer.setEnvironment(appMasterEnv);

      System.out.println("Set up resource type requirements for ApplicationMaster") ;
      Resource resource = Records.newRecord(Resource.class);
      resource.setMemory(256);
      resource.setVirtualCores(1);

      System.out.println("Finally, set-up ApplicationSubmissionContext for the application");
      ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
      appContext.setApplicationName(vmConfig.getName()); // application name
      appContext.setAMContainerSpec(amContainer);
      appContext.setResource(resource);
      appContext.setQueue("default"); // queue 

      // Submit application
      ApplicationId applicationId = appContext.getApplicationId();
      System.out.println("Submitting application " + applicationId);
      yarnClient.submitApplication(appContext);
    } catch(Exception ex) {
      ex.printStackTrace(); 
      throw ex ;
    }
  }
  
  public void uploadApp(VMConfig vmConfig) throws Exception {
    if(vmConfig.getDfsHome() == null) return ;
    HdfsConfiguration hdfsConf = new HdfsConfiguration() ;
    vmConfig.overrideYarnConfiguration(hdfsConf);
    FileSystem fs = FileSystem.get(hdfsConf);
    DistributedFileSystem dfs = (DistributedFileSystem)fs;
    Path appHomePath = new Path(vmConfig.getLocalHome()) ;
    Path appHomeSharePath = new Path(vmConfig.getDfsHome()) ;
    if(dfs.exists(appHomeSharePath)) {
      dfs.delete(appHomeSharePath, true) ;
    }
    dfs.copyFromLocalFile(false, true, appHomePath, appHomeSharePath);
  }
  
  Map<String, LocalResource> createLocalResources(Configuration conf, VMConfig appConfig) throws Exception {
    Map<String, LocalResource> libs = new HashMap<String, LocalResource>() ;
    FileSystem fs = FileSystem.get(conf) ;
    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(appConfig.getDfsHome() + "/libs"), true) ;
    while(itr.hasNext()) {
      FileStatus fstatus = itr.next() ;
      Path fpath = fstatus.getPath() ;
      LocalResource libJar = Records.newRecord(LocalResource.class);
      libJar.setResource(ConverterUtils.getYarnUrlFromPath(fpath));
      libJar.setSize(fstatus.getLen());
      libJar.setTimestamp(fstatus.getModificationTime());
      libJar.setType(LocalResourceType.FILE);
      libJar.setVisibility(LocalResourceVisibility.PUBLIC);
      libs.put(fpath.getName(), libJar) ;
    }
    return libs ;
  }
  
  static public void main(String[] args) throws Exception {
    AppClient appClient = new AppClient() ;
    appClient.run(args);
  }
}