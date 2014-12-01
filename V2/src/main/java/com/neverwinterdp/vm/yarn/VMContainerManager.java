package com.neverwinterdp.vm.yarn;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neverwinterdp.hadoop.yarn.app.AppConfig;
import com.neverwinterdp.hadoop.yarn.app.AppContainerInfoHolder;
import com.neverwinterdp.hadoop.yarn.app.master.AppMaster;
import com.neverwinterdp.hadoop.yarn.app.master.AppMasterContainerManager;
import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.VMServicePlugin;
import com.neverwinterdp.vm.master.VMManagerApplication;

public class VMContainerManager implements AppMasterContainerManager {
  protected static final Logger LOGGER = LoggerFactory.getLogger(VMContainerManager.class);
  
  private AppMaster appMaster ;
  private ContainerRequestQueue containerRequestQueue = new ContainerRequestQueue();
  
  public void onInit(final AppMaster appMaster) {
    this.appMaster = appMaster;
    AppConfig config = appMaster.getAppConfig() ;
    try {
      VMConfig vmConfig = new VMConfig() ;
      vmConfig.setName("vm-master-1");
      vmConfig.setRoles(new String[] {"vm-master"});
      VMDescriptor vmDescriptor = new VMDescriptor(vmConfig);
      VM vm = new VM(vmDescriptor);
      Map<String, String> properties = new HashMap<String, String>();
      VMManagerApplication vmApp = new VMManagerApplication() {
        protected void onInit(AppModule module) {
          module.bindInstance(VMServicePlugin.class, new YarnVMServicePlugin(VMContainerManager.this));
        }
      };
      vm.appStart(vmApp, properties);
    } catch(Exception ex) {
      LOGGER.error("failt to launch master vm", ex);
    }
  }
  
  public VMDescriptor allocate(VMService vmService, VMDescriptor vmDescriptor) throws RegistryException, Exception {
    LOGGER.info("Request allocate container");
    ContainerRequest containerReq = appMaster.createContainerRequest(0/*priority*/, 1/*core*/, 128/*memory*/);
    containerRequestQueue.offer(containerReq, vmDescriptor);
    appMaster.asyncAdd(containerReq) ;
    return vmDescriptor;
  }
  
  public void onRequestContainer(AppMaster appMaster) { }

  public void onAllocatedContainer(AppMaster master, Container container) {
    try {
      VMDescriptor vmDescriptor = containerRequestQueue.take(container);
      AppConfig config = master.getAppConfig() ;
      String command = "java " + VMMaster.class.getName();
      master.startContainer(container, command) ;
      LOGGER.info("Start container with command: " + vmDescriptor.getVmConfig().getName());
    } catch (YarnException e) {
      LOGGER.error("Error on start a container", e);
    } catch (IOException e) {
      LOGGER.error("Error on start a container", e);
    }
  }

  public void onCompleteContainer(AppMaster master, AppContainerInfoHolder containerInfo, ContainerStatus status) {
    
  }

  public void onFailedContainer(AppMaster master, AppContainerInfoHolder containerInfo, ContainerStatus status) {
  }

  public void onShutdownRequest(AppMaster appMaster)  {
  }
  
  public void onExit(AppMaster appMaster) {
    LOGGER.info("Finish onExit(AppMaster appMaster)");
  }
  
  public void waitForComplete(AppMaster appMaster) {
    LOGGER.info("Start waitForComplete(AppMaster appMaster)");
    try {
      Thread.sleep(30000);
    } catch (InterruptedException e) {
    }
    LOGGER.info("Finish waitForComplete(AppMaster appMaster)");
  }
  
  static public class ContainerRequestQueue {
    private Map<String, Queue<VMDescriptor>> multiQueues = new HashMap<String, Queue<VMDescriptor>>();
    
    synchronized public void offer(ContainerRequest request, VMDescriptor vmDescriptor) {
      String key = key(request.getCapability());
      Queue<VMDescriptor> queue = multiQueues.get(key);
      if(queue == null) {
        queue = new LinkedList<VMDescriptor>();
        multiQueues.put(key, queue);
      }
      queue.offer(vmDescriptor);
    }
    
    synchronized public VMDescriptor take(Container container) {
      Queue<VMDescriptor> queue = multiQueues.get(key(container.getResource()));
      return queue.poll();
    }
    
    private String key(Resource res) {
      String key = "cpu=" + res.getVirtualCores() + ",memory=" + res.getMemory();
      System.err.println("create key: " + key);
      return key;
    }
  }
}