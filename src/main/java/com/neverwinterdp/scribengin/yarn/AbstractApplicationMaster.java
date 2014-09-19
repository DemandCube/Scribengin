package com.neverwinterdp.scribengin.yarn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.beust.jcommander.Parameter;

public abstract class AbstractApplicationMaster {
  private AMRMClientAsync<ContainerRequest> resourceManager;
  private NMClient nodeManager;

  private Configuration conf;
  protected static final Logger LOG = Logger.getLogger(AbstractApplicationMaster.class.getName());

  @Parameter(names = {"-" + Constants.OPT_CONTAINER_MEM, "--" + Constants.OPT_CONTAINER_MEM})
  private int containerMem;

  @Parameter(names = {"-" + Constants.OPT_CONTAINER_COUNT, "--" + Constants.OPT_CONTAINER_COUNT})
  protected int totalContainerCount;

  @Parameter(names = {"-" + Constants.OPT_COMMAND, "--" + Constants.OPT_COMMAND})
  private String command;

  private AtomicInteger completedContainerCount;
  private AtomicInteger allocatedContainerCount;
  private AtomicInteger failedContainerCount;
  private AtomicInteger requestedContainerCount;

  private String appMasterHostname = "";     // TODO: What should this really be?
  private int appMasterRpcPort = 0;          // TODO: What should this really be?
  private String appMasterTrackingUrl = "";  // TODO: What should this really be?

  private boolean done;
  protected Map<ContainerId, String> containerIdCommandMap;
  protected List<String> failedCommandList;

  public AbstractApplicationMaster() {
    conf = new YarnConfiguration();
    completedContainerCount = new AtomicInteger();
    allocatedContainerCount = new AtomicInteger();
    failedContainerCount = new AtomicInteger();
    requestedContainerCount = new AtomicInteger();

    containerIdCommandMap = new HashMap<ContainerId, String>();
    failedCommandList = new ArrayList<String>();
  }

  public void init(String[] args) {
    LOG.setLevel(Level.INFO);
    done = false;
  }

  public boolean run() throws IOException, YarnException {
    // Initialize clients to RM and NMs.
    LOG.info("ApplicationMaster::run");
    LOG.error("command: " + this.command);
    AMRMClientAsync.CallbackHandler rmListener = new RMCallbackHandler();
    resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, rmListener);
    resourceManager.init(conf);
    resourceManager.start();

    nodeManager = NMClient.createNMClient();
    nodeManager.init(conf);
    nodeManager.start();

    appMasterHostname = "hdfs://localhost";
    appMasterRpcPort = 51484;
    appMasterTrackingUrl = "hdfs://localhost:51484";
    // Register with RM
    resourceManager.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);


    // Ask RM to give us a bunch of containers
    for (int i = 0; i < totalContainerCount; i++) {
      ContainerRequest containerReq = setupContainerReqForRM();
      resourceManager.addContainerRequest(containerReq);
    }
    requestedContainerCount.addAndGet(totalContainerCount);

    while (!done) {
      try {
        Thread.sleep(200);
      } catch (InterruptedException ex) {
      }
    }// while

    // Un-register with ResourceManager
    resourceManager.unregisterApplicationMaster( FinalApplicationStatus.SUCCEEDED, "", "");
    return true;
  }

  private ContainerRequest setupContainerReqForRM() {
    // Priority for worker containers - priorities are intra-application
    Priority priority = Records.newRecord(Priority.class);
    priority.setPriority(0);
    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMem);
    //capability.setVirtualCores(1);
    ContainerRequest containerReq = new ContainerRequest(
        capability,
        null /* hosts String[] */,
        null /* racks String [] */,
        priority);
    return containerReq;
  }

  private synchronized void recordFailedCommand(ContainerId cid) {
    String failedCmd = containerIdCommandMap.get(cid);
    containerIdCommandMap.remove(cid);
    failedCommandList.add(failedCmd);
  }

  abstract protected List<String> buildCommandList(int startingFrom, int containerCnt, String command);

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    // CallbackHandler for RM.
    // Execute a program when the container is allocated
    // Reallocate upon failure.


    public void onContainersCompleted(List<ContainerStatus> statuses) {
      for (ContainerStatus status: statuses) {
        assert (status.getState() == ContainerState.COMPLETE);

        int exitStatus = status.getExitStatus();
        if (exitStatus != ContainerExitStatus.SUCCESS) {
          if (exitStatus != ContainerExitStatus.ABORTED) {
            failedContainerCount.incrementAndGet();
          }
          allocatedContainerCount.decrementAndGet();
          requestedContainerCount.decrementAndGet();
          recordFailedCommand(status.getContainerId());
        } else {
          completedContainerCount.incrementAndGet();
        }
      }

      int askAgainCount = totalContainerCount - requestedContainerCount.get();
      requestedContainerCount.addAndGet(askAgainCount);

      if (askAgainCount > 0) {
        // need to reallocate failed containers
        for (int i = 0; i < askAgainCount; i++) {
          ContainerRequest req = setupContainerReqForRM();
          resourceManager.addContainerRequest(req);
        }
      }

      if (completedContainerCount.get() == totalContainerCount) {
        done = true;
      }
    }

    public void onContainersAllocated(List<Container> containers) {
      int containerCnt = containers.size();
      List<String> cmdLst;

      if (failedCommandList.isEmpty()) {
        int startFrom = allocatedContainerCount.getAndAdd(containerCnt);
        LOG.error("containerCnt: " + containerCnt);
        cmdLst = buildCommandList(startFrom, containerCnt, command);
      } else {
        // TODO: keep track of failed commands' history.
        cmdLst = failedCommandList;
        int failedCommandListCnt = failedCommandList.size();
        if (failedCommandListCnt < containerCnt) {
          // It's possible that the allocated containers are for both newly allocated and failed containers
          int startFrom = allocatedContainerCount.getAndAdd(containerCnt - failedCommandListCnt);
          cmdLst.addAll(buildCommandList(startFrom, containerCnt, command));
        }
      }

      for (int i = 0; i < containerCnt; i++) {
        Container c = containers.get(i);
        String cmdStr = cmdLst.remove(0);
        LOG.error("running cmd: " + cmdStr);
        StringBuilder sb = new StringBuilder();
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        containerIdCommandMap.put(c.getId(), cmdStr);
        ctx.setCommands(Collections.singletonList(
              sb.append(cmdStr)
                .append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout")
                .append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr")
                .toString()));
        try {
          nodeManager.startContainer(c, ctx);
        } catch (YarnException e) {
          // TODO: what should I do here? reallocated a new container?
        } catch (IOException e) {
          // TODO: what should I do here? reallocated a new container?
        }
      }
    }


    public void onNodesUpdated(List<NodeReport> updated) { }

    public void onError(Throwable e) {
      done = true;
      resourceManager.stop();
    }

    // Called when the ResourceManager wants the ApplicationMaster to shutdown
    // for being out of sync etc. The ApplicationMaster should not unregister
    // with the RM unless the ApplicationMaster wants to be the last attempt.
    public void onShutdownRequest() {
      done = true;
    }

    public float getProgress() {
      return 0;
    }
  }

}
