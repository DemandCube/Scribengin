package com.neverwinterdp.vm;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class VMConfig {
  
  @Inject @Named("vmresource.implementation")
  private String implementation ;

  public String getImplementation() { return implementation; }
  public void setImplementation(String implementation) { this.implementation = implementation; }
}
