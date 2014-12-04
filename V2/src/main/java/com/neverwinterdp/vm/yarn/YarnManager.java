package com.neverwinterdp.vm.yarn;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.hadoop.yarn.app.Util;
import com.neverwinterdp.vm.VMConfig;

@Singleton
public class YarnManager {
  private Logger logger = LoggerFactory.getLogger(YarnManager.class.getName());
  
  private AMRMClient<ContainerRequest> amrmClient ;
  private AMRMClientAsync<ContainerRequest> amrmClientAsync ;
  private NMClient nmClient;
  private Configuration conf;
  private ContainerRequestQueue containerRequestQueue = new ContainerRequestQueue ();

  public Configuration getConfiguration() { return this.conf ; }
  
  public AMRMClient<ContainerRequest> getAMRMClient() { return this.amrmClient ; }
  
  public NMClient getNMClient() { return this.nmClient ; }
  
  @Inject
  public void onInit(VMConfig vmConfig) throws Exception {
    logger.info("Start init(VMConfig vmConfig)");
    try {
      conf = new YarnConfiguration() ;
      vmConfig.overrideYarnConfiguration(conf);
      Iterator<Map.Entry<String, String>> i = conf.iterator();

      amrmClient = AMRMClient.createAMRMClient();
      amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(amrmClient, 1000, new AMRMCallbackHandler());
      amrmClientAsync.init(conf);
      amrmClientAsync.start();
      
      nmClient = NMClient.createNMClient();
      nmClient.init(conf);
      nmClient.start();
      // Register with RM
      RegisterApplicationMasterResponse registerResponse = amrmClientAsync.registerApplicationMaster("localhost", 0, "");
    } catch(Throwable t) {
      logger.error("Error: " , t);
    }
    logger.info("Finish init(VMConfig vmConfig)");
  }

  public void onDestroy() throws Exception {
    logger.info("Start onDestroy()");
    if(amrmClientAsync != null) {
      amrmClientAsync.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
      amrmClientAsync.stop();
      amrmClientAsync.close(); 
    }
    if(nmClient != null) {
      nmClient.stop();
      nmClient.close();
    }
    logger.info("Finish onDestroy()");
  }
  
  public ContainerRequest createContainerRequest(int priority, int numOfCores, int memory) {
    //Priority for worker containers - priorities are intra-application
    Priority containerPriority = Priority.newInstance(priority);
    //Resource requirements for worker containers
    Resource resource = Resource.newInstance(memory, numOfCores);
    ContainerRequest containerReq =  new ContainerRequest(resource, null /* hosts*/, null /*racks*/, containerPriority);
    return containerReq;
  }
  
  public void asyncAdd(ContainerRequest containerReq, ContainerRequestCallback callback) {
    logger.info("Start asyncAdd(ContainerRequest containerReq, ContainerRequestCallback callback)");
    containerRequestQueue.offer(containerReq, callback);
    amrmClientAsync.addContainerRequest(containerReq);
    logger.info("Finish asyncAdd(ContainerRequest containerReq, ContainerRequestCallback callback)");
  }
  
  public List<Container> getAllocatedContainers() throws YarnException, IOException {
    AllocateResponse response = amrmClient.allocate(0);
    return response.getAllocatedContainers() ;
  }
  
  public void startContainer(Container container, String command) throws YarnException, IOException {
    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    Util.setupAppMasterEnv(true, conf, appMasterEnv);
    ctx.setEnvironment(appMasterEnv);
    
    StringBuilder sb = new StringBuilder();
    List<String> commands = Collections.singletonList(
        sb.append(command).
        append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout").
        append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr").toString()
    );
    ctx.setCommands(commands);
    nmClient.startContainer(container, ctx);
    //TODO: update vm descriptor status
  }
  
  class AMRMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    public void onContainersCompleted(List<ContainerStatus> statuses) {
      logger.info("Start onContainersCompleted(List<ContainerStatus> statuses)");
      for (ContainerStatus status: statuses) {
        assert (status.getState() == ContainerState.COMPLETE);
        int exitStatus = status.getExitStatus();
        //TODO: update vm descriptor status
        if (exitStatus != ContainerExitStatus.SUCCESS) {
        } else {
        }
      }
      logger.info("Finish onContainersCompleted(List<ContainerStatus> statuses)");
    }

    public void onContainersAllocated(List<Container> containers) {
      logger.info("Start onContainersAllocated(List<Container> containers)");
      for (int i = 0; i < containers.size(); i++) {
        Container container = containers.get(i) ;
        containerRequestQueue.onAllocate(container);
      }
      logger.info("Finish onContainersAllocated(List<Container> containers)");
    }


    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onError(Throwable e) {
      amrmClientAsync.stop();
    }

    public void onShutdownRequest() {
      //TODO: handle shutdown request
    }

    public float getProgress() { return 0; }
  }
  
  static public interface ContainerRequestCallback {
    public void onRequest(ContainerRequest request) ;
    public void onAllocate(Container container) ;
  }
  
  static public class ContainerRequestQueue {
    private Map<String, Queue<ContainerRequestCallback>> multiQueues = new HashMap<String, Queue<ContainerRequestCallback>>();
    
    synchronized public void offer(ContainerRequest request, ContainerRequestCallback callback) {
      String key = key(request.getCapability());
      Queue<ContainerRequestCallback> queue = multiQueues.get(key);
      if(queue == null) {
        queue = new LinkedList<ContainerRequestCallback>();
        multiQueues.put(key, queue);
      }
      queue.offer(callback);
    }
    
    synchronized public void onAllocate(Container container) {
      Queue<ContainerRequestCallback> queue = multiQueues.get(key(container.getResource()));
      ContainerRequestCallback callback = queue.poll();
      callback.onAllocate(container);
    }
    
    private String key(Resource res) {
      String key = "cpu=" + res.getVirtualCores() + ",memory=" + res.getMemory();
      System.err.println("create key: " + key);
      return key;
    }
  }
}