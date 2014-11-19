package com.neverwinterdp.scribengin.vmresource.jvm;

import com.neverwinterdp.scribengin.vmresource.VMResourceService;
import com.neverwinterdp.scribengin.vmresource.VMResourceConfig;
import com.neverwinterdp.scribengin.vmresource.VMResourceFactory;

public class JVMVMResourceFactory implements VMResourceFactory {
  @Override
  public VMResourceService createAllocator(VMResourceConfig config) throws Exception {
    return new VMResourceServiceImpl();
  }
}
