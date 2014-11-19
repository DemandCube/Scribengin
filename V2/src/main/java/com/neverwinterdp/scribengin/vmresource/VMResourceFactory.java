package com.neverwinterdp.scribengin.vmresource;

public interface VMResourceFactory {
  public VMResourceAllocator createAllocator(VMResourceConfig config) throws Exception ;
}
