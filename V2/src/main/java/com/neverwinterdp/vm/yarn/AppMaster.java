package com.neverwinterdp.vm.yarn;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.hadoop.yarn.app.AppConfig;
import com.neverwinterdp.hadoop.yarn.app.AppContainerInfoHolder;
import com.neverwinterdp.hadoop.yarn.app.AppInfo;
import com.neverwinterdp.hadoop.yarn.app.Util;
import com.neverwinterdp.hadoop.yarn.app.protocol.IPCService;
import com.neverwinterdp.hadoop.yarn.app.history.AppHistorySender;
import com.neverwinterdp.hadoop.yarn.app.http.HttpService;
import com.neverwinterdp.hadoop.yarn.app.http.netty.NettyHttpService;
import com.neverwinterdp.hadoop.yarn.app.ipc.AppIPCService;
import com.neverwinterdp.hadoop.yarn.app.master.AppMasterContainerManager;
import com.neverwinterdp.netty.rpc.server.RPCServer;

public class AppMaster {
  static {
    System.setProperty("java.net.preferIPv4Stack", "true") ;
  }
  
  protected static final Logger LOGGER = LoggerFactory.getLogger(AppMaster.class.getName());
  
  private AppConfig appConfig;
  
  private AMRMClient<ContainerRequest> amrmClient ;
  private AMRMClientAsync<ContainerRequest> amrmClientAsync ;
  private NMClient nmClient;
  private Configuration conf;

  private RPCServer rpcServer ;
  private AppIPCService ipcService;
  private AppInfo  appInfo = new AppInfo();
  private AppMasterContainerManager containerManager ;
  
  //private WebApp webApp ;
  private HttpService httpService ;
  private AppHistorySender appHistorySender ;

  public AppMaster() {
  }
  
  public AppConfig getAppConfig() { return this.appConfig ; }
  
  public Configuration getConfiguration() { return this.conf ; }
  
  public AppInfo getAppInfo() { return this.appInfo ; }

  public AMRMClient<ContainerRequest> getAMRMClient() { return this.amrmClient ; }
  
  public NMClient getNMClient() { return this.nmClient ; }
  
  public AppIPCService getIPCServiceServer()  { return this.ipcService ; }
  
  public void run(String[] args) throws Exception {
    try {
      this.appConfig = new AppConfig() ;
      new JCommander(appConfig, args) ;
      appConfig.appStartTime = System.currentTimeMillis() ;
      appConfig.appState = "RUNNING" ;
      conf = new YarnConfiguration() ;
      appConfig.overrideConfiguration(conf);
      
      rpcServer = new RPCServer(appConfig.appRpcPort) ;
      rpcServer.startAsDeamon(); 
      
      ipcService = new AppIPCService(this) ;
      this.appConfig.appHostName = rpcServer.getHostIpAddress() ;
      this.appConfig.appRpcPort =  rpcServer.getPort() ;
      rpcServer.getServiceRegistry().register(IPCService.newReflectiveBlockingService(ipcService));
      
      httpService = new NettyHttpService(this) ;
      httpService.start();
      Thread.sleep(3000);
      appConfig.appTrackingUrl = httpService.getTrackingUrl() ;
      System.out.println("Tracking URL: " + appConfig.appTrackingUrl);
      Class<?> containerClass = Class.forName(appConfig.appContainerManager) ;
      containerManager = (AppMasterContainerManager)containerClass.newInstance() ;

      amrmClient = AMRMClient.createAMRMClient();
      amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(amrmClient, 1000, new AMRMCallbackHandler());
      amrmClientAsync.init(conf);
      amrmClientAsync.start();

      nmClient = NMClient.createNMClient();
      nmClient.init(conf);
      nmClient.start();
      
      appHistorySender = new AppHistorySender(this) ;
      
      containerManager.onInit(this);
      appHistorySender.startAutoSend(this);
      // Register with RM
      RegisterApplicationMasterResponse registerResponse = 
          amrmClientAsync.registerApplicationMaster(appConfig.appHostName, appConfig.appRpcPort, appConfig.appTrackingUrl);
      containerManager.onRequestContainer(this);
      containerManager.waitForComplete(this);
      containerManager.onExit(this);
    } catch(Throwable t) {
      LOGGER.error("Error: " , t);
    } finally {
      if(amrmClientAsync != null) {
        amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        amrmClientAsync.stop();
        amrmClientAsync.close(); 
      }
      if(nmClient != null) {
        nmClient.stop();
        nmClient.close();
      }

      if(httpService != null) {
        LOGGER.info("Shutdown the HttpService");
        httpService.shutdown() ; 
      }

      if(rpcServer != null) {
        rpcServer.shutdown();
      }
      
      appConfig.appState = "FINISHED" ;
      appConfig.appFinishTime = System.currentTimeMillis() ;
      if(appHistorySender != null) {
        LOGGER.info("Shutdown the AppHistorySender");
        appHistorySender.shutdown(); 
      }
      //IOUtil.save(JSONSerializer.INSTANCE.toString(appMonitor), "UTF-8", "/tmp/AppMonitor.json");
    }
    Thread.sleep(3000);
    Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
    for(Thread thread : threadSet) {
      System.out.println("Thread: " + thread.getName()) ;
      System.out.println("=============================================") ;
      for(StackTraceElement st : thread.getStackTrace()) {
        System.out.println(st.toString()) ;
      }
      System.out.println("\n\n");
    }
    if(appConfig.miniClusterEnv) System.exit(0);
  }

  public ContainerRequest createContainerRequest(int priority, int numOfCores, int memory) {
    //Priority for worker containers - priorities are intra-application
    Priority containerPriority = Priority.newInstance(priority);
    // Resource requirements for worker containers
    Resource resource = Resource.newInstance(memory, numOfCores);
    ContainerRequest containerReq =  new ContainerRequest(resource, null /* hosts*/, null /*racks*/, containerPriority);
    return containerReq;
  }
  
  public Container requestContainer(int priority, int numOfCores, int memory, long maxWait) throws YarnException, IOException, InterruptedException {
    ContainerRequest containerReq = createContainerRequest(priority, numOfCores, memory) ;
    amrmClient.addContainerRequest(containerReq);
    long stopTime = System.currentTimeMillis() + maxWait ;
    while (System.currentTimeMillis() < stopTime) {
      AllocateResponse response = amrmClient.allocate(0);
      List<Container> containers = response.getAllocatedContainers() ;
      if(containers.size() > 0)return containers.get(0) ;
      Thread.sleep(500);
    }
    return null;
  }
  
  public void asyncAdd(ContainerRequest containerReq) {
    amrmClientAsync.addContainerRequest(containerReq);
    appInfo.onContainerRequest(containerReq);
  }
  
  public void add(ContainerRequest containerReq) {
    amrmClient.addContainerRequest(containerReq);
    appInfo.onContainerRequest(containerReq);
  }
  
  public List<Container> getAllocatedContainers() throws YarnException, IOException {
    AllocateResponse response = amrmClient.allocate(0);
    return response.getAllocatedContainers() ;
  }
  
  public void startContainer(Container container) throws YarnException, IOException {
    startContainer(container, appConfig.buildWorkerCommand());
  }
  
  public void startContainer(Container container, String command) throws YarnException, IOException {
    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    Util.setupAppMasterEnv(true, conf, appMasterEnv);
    ctx.setEnvironment(appMasterEnv);
    
    appConfig.setAppWorkerContainerId(container.getId().getId());
    StringBuilder sb = new StringBuilder();
    List<String> commands = Collections.singletonList(
        sb.append(command).
        append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout").
        append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr")
        .toString()
        );
    ctx.setCommands(commands);
    appInfo.onAllocatedContainer(container, commands);
    nmClient.startContainer(container, ctx);
  }
  
  class AMRMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      for (ContainerStatus status: statuses) {
        assert (status.getState() == ContainerState.COMPLETE);
        int exitStatus = status.getExitStatus();
        AppContainerInfoHolder containerInfoHolder = appInfo.getAppContainerInfoHolder(status.getContainerId().getId()) ;
        if (exitStatus != ContainerExitStatus.SUCCESS) {
          appInfo.onFailedContainer(status);
          containerManager.onFailedContainer(AppMaster.this, containerInfoHolder, status);
        } else {
          appInfo.onCompletedContainer(status);
          containerManager.onCompleteContainer(AppMaster.this, containerInfoHolder, status);
        }
      }
    }

    public void onContainersAllocated(List<Container> containers) {
      for (int i = 0; i < containers.size(); i++) {
        Container container = containers.get(i) ;
        containerManager.onAllocatedContainer(AppMaster.this, container);
      }
    }


    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onError(Throwable e) {
      amrmClientAsync.stop();
    }

    public void onShutdownRequest() { 
      containerManager.onShutdownRequest(AppMaster.this); 
    }

    public float getProgress() { return 0; }
  }

  public AppMaster mock(AppConfig config) {
    this.appConfig = config ;
    return this ;
  }
  
  public AppMaster mock(AppInfo appInfo) {
    this.appInfo = appInfo ;
    return this ;
  }
  
  static public void main(String[] args) throws Exception {
    new AppMaster().run(args) ;
  }
}