package com.neverwinterdp.vm.jvm;

import org.junit.Assert;
import org.junit.Test;

import com.google.inject.Injector;
import com.neverwinterdp.scribengin.ScribenginUnitTest;
import com.neverwinterdp.vm.VMApplicationDummy;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.client.VMClient;
import com.neverwinterdp.vm.client.shell.Shell;
import com.neverwinterdp.vm.command.AppCommand;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.command.PingCommand;

public class VMServiceUnitTest extends ScribenginUnitTest {
  @Test
  public void testVMService() throws Exception {
    Injector container = newMasterContainer() ;
    VMService vmService = container.getInstance(VMService.class);
    for(int i = 0; i < 10; i++) {
      VMConfig vmConfig = new VMConfig();
      vmConfig.setName("test-" + i);
      vmConfig.setRoles(new String[] {"test"});
      VMDescriptor vmDescriptor = vmService.allocate(vmConfig) ;
      Command startCmd = new AppCommand.Start(VMApplicationDummy.class.getName(), new String[] {"Dummy-" + i}) ;
      CommandResult<?> startCmdResult = vmService.getVMClient().execute(vmDescriptor, startCmd);
      Assert.assertTrue(startCmdResult.getResultAs(Boolean.class));
    }
    Thread.sleep(2000);
    VMDescriptor[] allocatedVMs = vmService.getAllocatedVMs();
    Assert.assertEquals(10, allocatedVMs.length);
    
    Shell shell = newShell();
    shell.execute("vm list");
    
    VMClient vmClient = shell.getVMClient();
    for(int i = 0; i < allocatedVMs.length; i++) {
      VMDescriptor vmDescriptor = allocatedVMs[i];
      CommandResult<?> result = vmClient.execute(vmDescriptor, new PingCommand("Hello Ping"));
      String pingResult = result.getResultAs(String.class);
      Assert.assertEquals("Got your message: Hello Ping", pingResult) ;
    }
    
    for(int i = 0; i < allocatedVMs.length; i++) {
      VMDescriptor vmDescriptor = allocatedVMs[i];
      vmService.release(vmDescriptor);
    }
  }
}