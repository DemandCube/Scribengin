package com.neverwinterdp.vm.environment.yarn;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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

  private VMConfig vmConfig ;
  private Map<String, String> yarnConfig;

  private Configuration conf ;
  private AMRMClient<ContainerRequest> amrmClient ;
  private AMRMClientAsync<ContainerRequest> amrmClientAsync ;
  private NMClient nmClient;
  private ContainerRequestQueue containerRequestQueue = new ContainerRequestQueue ();
  
  public Map<String, String> getYarnConfig() { return this.yarnConfig ; }
  
  public AMRMClient<ContainerRequest> getAMRMClient() { return this.amrmClient ; }
  
  public NMClient getNMClient() { return this.nmClient ; }
  
  @Inject
  public void onInit(VMConfig vmConfig) throws Exception {
    logger.info("Start init(VMConfig vmConfig)");
    this.vmConfig = vmConfig;
    try {
      this.yarnConfig = vmConfig.getYarnConf();
      conf = new YarnConfiguration() ;
      vmConfig.overrideYarnConfiguration(conf);
      
      amrmClient = AMRMClient.createAMRMClient();
      amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(amrmClient, 1000, new AMRMCallbackHandler());
      amrmClientAsync.init(conf);
      amrmClientAsync.start();
      
      nmClient = NMClient.createNMClient();
      nmClient.init(conf);
      nmClient.start();
      // Register with RM
      String appHostName = InetAddress.getLocalHost().getHostAddress()  ;

      RegisterApplicationMasterResponse registerResponse = amrmClientAsync.registerApplicationMaster(appHostName, 0, "");
      System.out.println("amrmClientAsync.registerApplicationMaster");
    } catch(Throwable t) {
      logger.error("Error: " , t);
      t.printStackTrace();
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
    System.err.println("Start asyncAdd(ContainerRequest containerReq, ContainerRequestCallback callback)");
    System.err.println(" container request hash code = " + containerReq.hashCode());
    containerReq.setCallback(callback);
    containerRequestQueue.offer(containerReq);
    amrmClientAsync.addContainerRequest(containerReq);
    System.err.println("Finish asyncAdd(ContainerRequest containerReq, ContainerRequestCallback callback)");
    logger.info("Finish asyncAdd(ContainerRequest containerReq, ContainerRequestCallback callback)");
  }
  
  public List<Container> getAllocatedContainers() throws YarnException, IOException {
    AllocateResponse response = amrmClient.allocate(0);
    return response.getAllocatedContainers() ;
  }
  
  public void startContainer(Container container, String command) throws YarnException, IOException {
    ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
    if(vmConfig.getVmResources().size() > 0) {
      ctx.setLocalResources(new VMResources(conf, vmConfig));
    }
    Map<String, String> appMasterEnv = new HashMap<String, String>();
    boolean jvmEnv = vmConfig.getEnvironment() != VMConfig.Environment.YARN;
    Util.setupAppMasterEnv(jvmEnv , conf, appMasterEnv);
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
      System.err.println("Start onContainersAllocated(List<Container> containers)");
      //TODO: review on allocated container code
      Container container = containers.get(0) ;
      ContainerRequest containerReq = containerRequestQueue.take(container);
      if(containerReq ==null) {
        //TODO: research on this issue
        //http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/yarn/client/api/AMRMClient.html#removeContainerRequest(T)
        return;
      }
      System.err.println(" container request hash code = " + containerReq.hashCode());
      containerReq.getCallback().onAllocate(YarnManager.this, containerReq, container);
      amrmClientAsync.removeContainerRequest(containerReq);
      
//      for (int i = 0; i < containers.size(); i++) {
//        System.err.println("  container " + i);
//        Container container = containers.get(i) ;
//        ContainerRequestCallback callback = containerRequestQueue.take(container);
//        callback.onAllocate(YarnManager.this, container);
//        amrmClientAsync.removeContainerRequest(callback.getContainerRequest());
//      }
      System.err.println("Finish onContainersAllocated(List<Container> containers)");
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
  
  class ContainerRequestQueue {
    private List<ContainerRequest> queues = new ArrayList<>();
    
    synchronized public void offer(ContainerRequest request) {
      queues.add(request);
    }
    
    synchronized public ContainerRequest take(Container container) {
      ContainerRequest containerReq = null ;
      System.err.println("  take for container " + container) ;
      System.err.println("    callback in queues " + queues.size()) ;
      int cpuCores = container.getResource().getVirtualCores();
      int memory = container.getResource().getMemory();
      System.err.println("    container allocate cpu  " + cpuCores) ;
      System.err.println("    container allocate memory " + memory) ;
      for(int i = 0; i < queues.size(); i++) {
        ContainerRequest sel = queues.get(i);
        System.err.println("    check container request cpu = " + sel.getCapability().getVirtualCores()) ;
        if(cpuCores < sel.getCapability().getVirtualCores()) continue;
        System.err.println("    check container request memory = " + sel.getCapability().getMemory()) ;
        if(memory < sel.getCapability().getMemory()) continue;
        if(containerReq == null) {
          containerReq = sel;
        } else {
          int callbackMemory = containerReq.getCapability().getMemory();
          int callbackCpuCores = containerReq.getCapability().getVirtualCores();
          //Select closest match memory and cpu cores requirement
          if(sel.getCapability().getMemory() < callbackMemory && 
             sel.getCapability().getVirtualCores() < callbackCpuCores) {
            containerReq = sel;
            continue;
          }
          if(sel.getCapability().getVirtualCores() < callbackCpuCores) {
            containerReq = sel;
            continue;
          }
          if(sel.getCapability().getMemory() < callbackMemory) {
            containerReq = sel;
            continue;
          }
        }
      }
      if(containerReq != null) queues.remove(containerReq);
      return containerReq;
    }
  }
  
  static public class ContainerRequest extends org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest {
    static public AtomicLong idTracker = new AtomicLong() ;
    private long id ;
    private ContainerRequestCallback callback ;
    
    public ContainerRequest(Resource capability, String[] nodes, String[] racks, Priority priority) {
      super(capability, nodes, racks, priority);
      id = idTracker.getAndIncrement();
    }

    public long getId() { return id; }

    public ContainerRequestCallback getCallback() { return callback; }
    public void setCallback(ContainerRequestCallback callback) {
      this.callback = callback;
    }
  }
  
  static public interface ContainerRequestCallback {
    public void onAllocate(YarnManager manager, ContainerRequest request, Container container) ;
  }
}