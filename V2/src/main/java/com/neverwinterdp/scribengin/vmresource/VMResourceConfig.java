package com.neverwinterdp.scribengin.vmresource;

import com.beust.jcommander.Parameter;

public class VMResourceConfig {
  @Parameter(names = {"--vm-resource-factory"}, description = "The factory class to create the vm resource allocator")
  private String factory ;

  public String getFactory() { return factory; }
  public void setFactory(String factory) { this.factory = factory; }
}
