package com.neverwinterdp.vm.environment.yarn;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.util.Records;

import com.neverwinterdp.hadoop.yarn.app.Util;
import com.neverwinterdp.vm.HadoopProperties;
import com.neverwinterdp.vm.VMConfig;

public class AppClient {
  private HadoopProperties hadoopProperties ;
  
  public AppClient(HadoopProperties hadoopProperties) {
    this.hadoopProperties = hadoopProperties;
  }

  public void run(VMConfig vmConfig, Configuration conf) throws Exception {
    try {
      vmConfig.overrideHadoopConfiguration(conf);
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
      if(vmConfig.getVmResources().size() > 0) {
        amContainer.setLocalResources(new VMResources(conf, vmConfig));
      }
      
      System.out.println("Setup the classpath for ApplicationMaster, environment = " + vmConfig.getEnvironment()) ;
      Map<String, String> appMasterEnv = new HashMap<String, String>();
      boolean jvmEnv = vmConfig.getEnvironment() != VMConfig.Environment.YARN;
      Util.setupAppMasterEnv(jvmEnv , conf, appMasterEnv);
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
  
  public void uploadApp(String localAppHome, String dfsAppHome) throws Exception {
    if(dfsAppHome == null || localAppHome == null) return; 
    HdfsConfiguration hdfsConf = new HdfsConfiguration() ;
    hadoopProperties.overrideConfiguration(hdfsConf);
    FileSystem fs = FileSystem.get(hdfsConf);
    DistributedFileSystem dfs = (DistributedFileSystem)fs;
    Path appHomePath = new Path(localAppHome) ;
    Path appHomeSharePath = new Path(dfsAppHome) ;
    if(dfs.exists(appHomeSharePath)) {
      dfs.delete(appHomeSharePath, true) ;
    }
    dfs.copyFromLocalFile(false, true, appHomePath, appHomeSharePath);
    HDFSUtil.dump(dfs, "/");
  }
}