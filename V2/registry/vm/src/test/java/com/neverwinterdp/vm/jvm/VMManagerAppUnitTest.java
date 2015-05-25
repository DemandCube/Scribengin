package com.neverwinterdp.vm.jvm;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.registry.event.WaitingNodeEventListener;
import com.neverwinterdp.registry.event.WaitingRandomNodeEventListener;
import com.neverwinterdp.registry.util.RegistryDebugger;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.util.text.TabularFormater;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMDummyApp;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.event.VMWaitingEventListener;
import com.neverwinterdp.vm.service.VMService;
import com.neverwinterdp.vm.service.VMServiceCommand;
import com.neverwinterdp.vm.tool.VMZKClusterBuilder;
import com.neverwinterdp.vm.util.VMNodeDebugger;

public class VMManagerAppUnitTest  {
  static {
    System.setProperty("log4j.configuration", "file:src/test/resources/test-log4j.properties") ;
  }
  
  VMZKClusterBuilder  vmCluster ;
  Shell      shell;
  VMClient   vmClient;
  private    RegistryDebugger debugger ;
  
  @Before
  public void setup() throws Exception {
    vmCluster = new VMZKClusterBuilder() ;
    vmCluster.clean();
    vmCluster.starZookeeper();
    Thread.sleep(5000);
    
    debugger = new RegistryDebugger(System.err, vmCluster.getVMClient().getRegistry().connect());
    debugger.watch("/vm/all/vm-master-1", new VMNodeDebugger(), true);
  }
  
  @After
  public void teardown() throws Exception {
    vmCluster.shutdown();
  }
  
  @Test
  public void testMaster() throws Exception {
    try {
      WaitingNodeEventListener master1waitingListener = vmCluster.createVMMaster("vm-master-1");
      master1waitingListener.waitForEvents(5000);
      TabularFormater info = master1waitingListener.getTabularFormaterEventLogInfo();
      info.setTitle("Waiting for vm-master events to make sure it is launched properly");
      System.out.println(info.getFormatText()); 
      
      vmCluster.createVMMaster("vm-master-2");
      Thread.sleep(2000);
      
      vmClient = vmCluster.getVMClient();
      shell = new Shell(vmClient) ;
      shell.execute("registry dump");
      
      VMWaitingEventListener eventsListener = new VMWaitingEventListener(shell.getVMClient().getRegistry());
      banner("Create VM Dummy 1");
      
      WaitingNodeEventListener vmDummy1WaitingEventListener = createVMWaitingNodeEventListener("vm-dummy-1");
      VMDescriptor vmDummy1 = allocate(vmClient, "vm-dummy-1") ;
      vmDummy1WaitingEventListener.waitForEvents(5000);

      shell.execute("registry dump");

      banner("Shutdown VM Master 1");
      //shutdown vm master 1 , the vm-master-2 should pickup the leader role.
      WaitingNodeEventListener shutdownVMMaster1WaitingListener = new WaitingRandomNodeEventListener(vmClient.getRegistry()) ;
      shutdownVMMaster1WaitingListener.add(VMService.getVMStatusPath("vm-master-1"), VMStatus.TERMINATED, "Wait for TERMINATED status for vm-master-1", true);
      shutdownVMMaster1WaitingListener.addDelete(VMService.getVMHeartbeatPath("vm-master-1"), "Expect  delete heartbeat for vm-master-1", true);
      //TODO: add assert vm-master-2 become leader
      vmClient.shutdown(vmClient.getMasterVMDescriptor());
      shutdownVMMaster1WaitingListener.waitForEvents(5000);
      shell.execute("registry dump");

      banner("Create VM Dummy 2");
      WaitingNodeEventListener vmDummy2WaitingEventListener = createVMWaitingNodeEventListener("vm-dummy-2");
      VMDescriptor vmDummy2 = allocate(vmClient, "vm-dummy-2") ;
      vmDummy2WaitingEventListener.waitForEvents(5000);
      shell.execute("registry dump");

      banner("Shutdown VM Dummy 1 and 2");
      WaitingNodeEventListener shutdownDummysWaitingListener = new WaitingRandomNodeEventListener(vmClient.getRegistry()) ;
      shutdownDummysWaitingListener.add(VMService.getVMStatusPath("vm-dummy-1"), VMStatus.TERMINATED, "Wait for TERMINATED status for vm-dummy-1", true);
      shutdownDummysWaitingListener.add(VMService.getVMStatusPath("vm-dummy-2"), VMStatus.TERMINATED, "Wait for TERMINATED status for vm-dummy-2", true);
      
      Assert.assertTrue(shutdown(vmClient, vmDummy2));
      Assert.assertTrue(shutdown(vmClient, vmDummy1));
      shutdownDummysWaitingListener.waitForEvents(10000);
      
      vmClient.shutdown();
    } catch(Exception ex) {
      ex.printStackTrace();
    } finally {
      Thread.sleep(1000);
      shell.execute("registry dump");
      shell.execute("help");
    }
  }

  private void banner(String title) {
    System.out.println("\n\n");
    System.out.println("------------------------------------------------------------------------");
    System.out.println(title);
    System.out.println("------------------------------------------------------------------------");
  }
  
  private VMDescriptor allocate(VMClient vmClient, String vmId) throws Exception {
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    String[] args = {
      "--name", vmId,
      "--role", "dummy",
      "--registry-connect", "127.0.0.1:2181", 
      "--registry-db-domain", "/NeverwinterDP",
      "--registry-implementation", RegistryImpl.class.getName(),
      "--vm-application", VMDummyApp.class.getName()
    };
    
    VMConfig vmConfig = new VMConfig() ;
    new JCommander(vmConfig, args);
    CommandResult<?> result = vmClient.execute(masterVMDescriptor, new VMServiceCommand.Allocate(vmConfig));
    Assert.assertNull(result.getErrorStacktrace());
    VMDescriptor vmDescriptor = result.getResultAs(VMDescriptor.class);
    Assert.assertNotNull(vmDescriptor);
    return vmDescriptor;
  }
  
  private WaitingNodeEventListener createVMWaitingNodeEventListener(String vmId) throws Exception {
    WaitingNodeEventListener waitingListener = new WaitingRandomNodeEventListener(vmClient.getRegistry()) ;
    String vmStatusPath = VMService.getVMStatusPath(vmId);
    waitingListener.add(vmStatusPath, VMStatus.RUNNING, "Wait for RUNNING status for vm " + vmId, true);
    String vmHeartbeatPath = VMService.getVMHeartbeatPath(vmId);
    waitingListener.addCreate(vmHeartbeatPath, "Expect  heartbeat for " + vmId, true);
    return waitingListener;
  }
  
  private boolean shutdown(VMClient vmClient, VMDescriptor vmDescriptor) throws Exception {
    CommandResult<?> result = vmClient.execute(vmDescriptor, new VMCommand.Shutdown());
    Assert.assertNull(result.getErrorStacktrace());
    return result.getResultAs(Boolean.class);
  }
}