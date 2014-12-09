package com.neverwinterdp.scribengin;


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.beust.jcommander.JCommander;
import com.neverwinterdp.registry.zk.RegistryImpl;
import com.neverwinterdp.scribengin.client.shell.ScribenginShell;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.scribengin.vm.VMScribenginCommand;
import com.neverwinterdp.scribengin.vm.VMScribenginMasterApp;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMServicePlugin;
import com.neverwinterdp.vm.VMUnitTest;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.command.VMCommand;
import com.neverwinterdp.vm.jvm.JVMVMServicePlugin;
import com.neverwinterdp.vm.master.VMManagerApp;
import com.neverwinterdp.vm.master.command.VMMasterCommand;

public class VMScribenginMasterAppUnitTest extends VMUnitTest {

  @Before
  public void setup() throws Exception {
    super.setup();
  }
  
  @After
  public void teardown() throws Exception {
    super.teardown();
  }
  
  @Test
  public void testMaster() throws Exception {
    banner("Create VM Master 1");
    VM vmMaster1 = createVMMaster("vm-master-1");
    Thread.sleep(500);
   
    Thread.sleep(1000);
    ScribenginShell shell = new ScribenginShell(newRegistry().connect());
    VMClient vmClient = shell.getVMClient();
    shell.execute("vm list");
    shell.execute("registry dump");

    banner("Create Scribengin Master");
    VMDescriptor scribenginMaster1 = createVMScribenginMaster(vmClient, "vm-scribengin-master-1") ;
    Thread.sleep(2000);
    shell.execute("vm list");
    shell.execute("registry dump --path /");
    
    banner("Create Scribengin Master");
    VMDescriptor scribenginMaster2 = createVMScribenginMaster(vmClient, "vm-scribengin-master-2") ;
    Thread.sleep(2000);
    
    VMDescriptor scribenginMaster = shell.getScribenginClient().getScribenginMaster();
    DataflowDescriptor dflDescriptor = new DataflowDescriptor();
    dflDescriptor.setName("test-dataflow");
    Command deployCmd = new VMScribenginCommand.DataflowDeployCommand(dflDescriptor) ;
    CommandResult<Boolean> result = 
        (CommandResult<Boolean>)vmClient.execute(scribenginMaster, deployCmd, 35000);
    Assert.assertTrue(result.getResult());
    
    Thread.sleep(1000);
    shell.execute("vm list");
    shell.execute("registry dump --path /");
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
      "--vm-application",VMManagerApp.class.getName(),
      "--prop:implementation:" + VMServicePlugin.class.getName() + "=" + JVMVMServicePlugin.class.getName()
    };
    VM vm = VM.run(args);
    return vm;
  }
  
  private VMDescriptor createVMScribenginMaster(VMClient vmClient, String name) throws Exception {
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    String[] args = {
      "--name", name,
      "--roles", "scribengin-master",
      "--registry-connect", "127.0.0.1:2181", 
      "--registry-db-domain", "/NeverwinterDP", 
      "--registry-implementation", RegistryImpl.class.getName(),
      "--vm-application", VMScribenginMasterApp.class.getName()
    };
    VMConfig vmConfig = new VMConfig() ;
    new JCommander(vmConfig, args);
    CommandResult<?> result = vmClient.execute(masterVMDescriptor, new VMMasterCommand.Allocate(vmConfig));
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