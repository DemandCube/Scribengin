package com.neverwinterdp.vm.master;


import java.util.HashMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.neverwinterdp.module.AppModule;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.ScribenginUnitTest;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMServicePlugin;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.jvm.JVMVMServicePlugin;
import com.neverwinterdp.vm.master.command.VMMasterCommand;

public class VMManagerApplicationUnitTest extends ScribenginUnitTest {

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
    VM vmMaster1 = createVMMaster("vm-master-1");
    Thread.sleep(500);
    VM vmMaster2 = createVMMaster("vm-master-2");
   
    Thread.sleep(1000);
    Shell shell = newShell();
    shell.execute("vm list");
    shell.execute("registry dump");
    
    Registry registry = newRegistry().connect() ;
    VMClient vmClient = new VMClient(registry);
    VMDescriptor sMaster1 = allocate(vmClient, "scribengin-master-1") ;
    shell.execute("vm list");
    
    //shutdown vm master 1 , the vm-master-2 should pickup the leader role.
    vmMaster1.exit();
    Thread.sleep(300);
    shell.execute("registry dump");
    
    VMDescriptor sMaster2 = allocate(vmClient, "scribengin-master-2") ;
    shell.execute("vm list");
    Assert.assertTrue(release(vmClient, sMaster1));
    Thread.sleep(1000);
    shell.execute("vm list");
    shell.execute("registry dump");
  }
  
  private VM createVMMaster(String name) throws Exception {
    VMConfig vmConfig = new VMConfig() ;
    vmConfig.setName(name);
    vmConfig.setRoles(new String[] {"vm-master"});
    VM vm = new VM(vmConfig);
    VMManagerApplication vmApp = new VMManagerApplication() {
      @Override
      protected void onInit(AppModule module) {
        module.bindInstance(VMServicePlugin.class, new JVMVMServicePlugin());
      }
    };
    vm.appStart(vmApp, new HashMap<String, String>());
    return vm;
  }
  
  private VMDescriptor allocate(VMClient vmClient, String name) throws Exception {
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    VMConfig scribenginMasterVMConfig = new VMConfig() ;
    scribenginMasterVMConfig.setName(name);
    scribenginMasterVMConfig.setRoles(new String[] {"scribengin-master"});
    CommandResult<?> result = vmClient.execute(masterVMDescriptor, new VMMasterCommand.Allocate(scribenginMasterVMConfig));
    Assert.assertNull(result.getErrorStacktrace());
    VMDescriptor vmDescriptor = result.getResultAs(VMDescriptor.class);
    Assert.assertNotNull(vmDescriptor);
    return vmDescriptor;
  }
  
  private boolean release(VMClient vmClient, VMDescriptor vmDescriptor) throws Exception {
    VMDescriptor masterVMDescriptor = vmClient.getMasterVMDescriptor();
    CommandResult<?> result = vmClient.execute(masterVMDescriptor, new VMMasterCommand.Release(vmDescriptor));
    Assert.assertNull(result.getErrorStacktrace());
    return result.getResultAs(Boolean.class);
    
  }
  
}