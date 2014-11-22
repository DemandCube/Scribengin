package com.neverwinterdp.vm.jvm;

import org.junit.Assert;
import org.junit.Test;

import com.google.inject.Injector;
import com.neverwinterdp.scribengin.ScribenginUnitTest;
import com.neverwinterdp.scribengin.client.shell.Shell;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMService;
import com.neverwinterdp.vm.app.VMApplicationLogic;

public class VMResourceServiceUnitTest extends ScribenginUnitTest {
  @Test
  public void testAllocator() throws Exception {
    Injector container = newMasterContainer() ;
    VMService allocator = container.getInstance(VMService.class);
    for(int i = 0; i < 10; i++) {
      VM vmResource = allocator.allocate(1, 128) ;
      vmResource.startApp(DummyVMApplication.class.getName(), new String[] {"Dummy-" + i});
    }
    Thread.sleep(5000);
    VM[] allocatedVMResources = allocator.getAllocatedVMResources();
    Assert.assertEquals(10, allocatedVMResources.length);
    
    Shell shell = newShell();
    shell.execute("vmresource list");
    
    for(int i = 0; i < allocatedVMResources.length; i++) {
      VM vmResource = allocatedVMResources[i];
      allocator.release(vmResource);
    }
  }
  
  static public class DummyVMApplication implements VMApplicationLogic {
    @Override
    public void run(String[] args) throws Exception {
      String name = args[0];
      try {
        while(true) {
          Thread.sleep(1000);
          System.out.println("Hello " + name);
        }
      } catch(InterruptedException ex) {
        
      }
    }
  }
}