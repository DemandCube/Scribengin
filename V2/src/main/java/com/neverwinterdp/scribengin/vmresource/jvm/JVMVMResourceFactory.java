package com.neverwinterdp.scribengin.vmresource.jvm;

import com.neverwinterdp.scribengin.vmresource.VMResourceAllocator;
import com.neverwinterdp.scribengin.vmresource.VMResourceConfig;
import com.neverwinterdp.scribengin.vmresource.VMResourceFactory;

public class JVMVMResourceFactory implements VMResourceFactory {
  @Override
  public VMResourceAllocator createAllocator(VMResourceConfig config) throws Exception {
    return new VMResourceAllocatorImpl();
  }
}
