package com.neverwinterdp.vm.jvm;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMDummyApp;
import com.neverwinterdp.vm.VMStatus;
import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.environment.jvm.JVMVMServicePlugin;
import com.neverwinterdp.vm.junit.VMAssert;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMServicePlugin;
import com.neverwinterdp.vm.service.command.VMServiceCommand;

public class VMManagerAppUnitTest extends VMUnitTest {
  Shell shell;
  VMClient vmClient;
  
  @Before
  public void setup() throws Exception {
    super.setup();
    Thread.sleep(2000);
    shell = newShell();
    vmClient = shell.getVMClient();
  }
  
  @After
  public void teardown() throws Exception {
    super.teardown();
  }
  
  @Test
  public void testMaster() throws Exception {
    VMAssert vmAssert = new VMAssert(shell.getVMClient());
    vmAssert.assertVMStatus("Expect vm-master-1 with running status", "vm-master-1", VMStatus.RUNNING, true);
    vmAssert.assertVMStatus("Expect vm-master-2 with running status", "vm-master-2", VMStatus.RUNNING, true);
    
    banner("Create Masters");
    VM vmMaster1 = createVMMaster("vm-master-1");
    VM vmMaster2 = createVMMaster("vm-master-2");
    vmAssert.waitForEvents(5000);
    
    shell.execute("vm list");
    shell.execute("registry dump");

    banner("Create VM Dummy 1");
    vmAssert.assertVMStatus("Expect vm-dummy-1 with running status", "vm-dummy-1", VMStatus.RUNNING, true);
    VMDescriptor vmDummy1 = allocate(vmClient, "vm-dummy-1") ;
    vmAssert.waitForEvents(5000);
    shell.execute("vm list");
    
    banner("Shutdown VM Master 1");
    //shutdown vm master 1 , the vm-master-2 should pickup the leader role.
    vmMaster1.shutdown();
    Thread.sleep(1000);
    shell.execute("registry dump");
    
    banner("Create VM Dummy 2");
    VMDescriptor vmDummy2 = allocate(vmClient, "vm-dummy-2") ;
    shell.execute("vm list");
    shell.execute("vm history");
    
    banner("Shutdown VM Dummy 2");
    Assert.assertTrue(shutdown(vmClient, vmDummy2));
    Thread.sleep(500);
    shell.execute("vm list");
    shell.execute("vm history");

    Assert.assertTrue(shutdown(vmClient, vmDummy1));
    Thread.sleep(3000);
    shell.execute("vm list");
    shell.execute("registry dump");
  }

  private void banner(String title) {
    System.out.println("\n\n");
    System.out.println("------------------------------------------------------------------------");
    System.out.println(title);
    System.out.println("------------------------------------------------------------------------");
  }
  
  private VM createVMMaster(String name) throws Exception {
    String[] args = {
      "--name", name,
      "--roles", "vm-master",
      "--self-registration",
      "--registry-connect", "127.0.0.1:2181", 
      "--registry-db-domain", "/NeverwinterDP", 
      "--registry-implementation", RegistryImpl.class.getName(),
      "--vm-application",VMServiceApp.class.getName(),
      "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + JVMVMServicePlugin.class.getName()
    };
    VM vm = VM.run(args);
    return vm;
  }
  
  private VMDescriptor allocate(VMClient vmClient, String name) throws Exception {
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    String[] args = {
      "--name", name,
      "--roles", "dummy",
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
  
  private boolean shutdown(VMClient vmClient, VMDescriptor vmDescriptor) throws Exception {
    CommandResult<?> result = vmClient.execute(vmDescriptor, new VMCommand.Shutdown());
    Assert.assertNull(result.getErrorStacktrace());
    return result.getResultAs(Boolean.class);
  }
}