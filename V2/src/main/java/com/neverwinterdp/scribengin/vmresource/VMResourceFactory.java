package com.neverwinterdp.scribengin.vmresource;

public interface VMResourceFactory {
  public VMResourceService createAllocator(VMResourceConfig config) throws Exception ;
}
