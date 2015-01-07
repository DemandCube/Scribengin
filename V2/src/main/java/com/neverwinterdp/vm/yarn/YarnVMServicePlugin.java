package com.neverwinterdp.vm.yarn;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.VMServicePlugin;

@Singleton
public class YarnVMServicePlugin implements VMServicePlugin {
  private Logger logger = LoggerFactory.getLogger(YarnVMServicePlugin.class);
  
  @Inject
  private YarnManager yarnManager;
  
  @Override
  synchronized public void allocate(VMService vmService, final VMConfig vmConfig) throws RegistryException, Exception {
    logger.info("Start allocate(VMService vmService, VMDescriptor vmDescriptor)");
    final ContainerRequest containerReq = 
        yarnManager.createContainerRequest(0, vmConfig.getRequestCpuCores(), vmConfig.getRequestMemory());
    YarnManager.ContainerRequestCallback callback = new YarnManager.ContainerRequestCallback() {
      private ContainerRequest containerRequest;

      @Override
      public ContainerRequest getContainerRequest() { return containerRequest; }
      
      @Override
      public void onRequest(YarnManager manager, ContainerRequest request) {
        this.containerRequest = request;
      }

      @Override
      public void onAllocate(YarnManager manager, Container container) {
        logger.info("Start onAllocate(Container container)");
        vmConfig.
          setSelfRegistration(false).
          addYarnProperty(manager.getYarnConfig());
        try {
          yarnManager.startContainer(container, vmConfig.buildCommand());
        } catch (YarnException | IOException e) {
          logger.error("Cannot start the container", e);
        }
        logger.info("Finish onAllocate(Container container)");
      }
      
      public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("Command: " + vmConfig.buildCommand());
        return b.toString();
      }
    };
    yarnManager.asyncAdd(containerReq, callback);
    logger.info("Finish allocate(VMService vmService, VMDescriptor vmDescriptor)");
  }

  @Override
  synchronized public void onKill(VMService vmService, VMDescriptor vmDescriptor) throws Exception {
    logger.info("Start onKill(VMService vmService, VMDescriptor vmDescriptor)");
    logger.info("Finish onKill(VMService vmService, VMDescriptor vmDescriptor)");
  }
}