package com.neverwinterdp.scribengin.vmresource.jvm;

import org.junit.Assert;
import org.junit.Test;

import com.neverwinterdp.scribengin.vmresource.VMApplication;
import com.neverwinterdp.scribengin.vmresource.VMResource;

public class VMResourceAllocatorUnitTest {
  @Test
  public void testAllocator() throws Exception {
    VMResourceAllocatorImpl allocator = new VMResourceAllocatorImpl();
    for(int i = 0; i < 10; i++) {
      VMResource vmResource = allocator.allocate(1, 128) ;
      vmResource.startApp(DummyVMApplication.class.getName(), new String[] {"Dummy-" + i});
    }
    Thread.sleep(5000);
    VMResource[] allocatedVMResources = allocator.getAllocatedVMResources();
    Assert.assertEquals(10, allocatedVMResources.length);
    for(int i = 0; i < allocatedVMResources.length; i++) {
      VMResource vmResource = allocatedVMResources[i];
      allocator.release(vmResource);
    }
  }
  
  static public class DummyVMApplication implements VMApplication {
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