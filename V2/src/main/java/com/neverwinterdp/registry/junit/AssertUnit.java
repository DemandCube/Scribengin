package com.neverwinterdp.registry.junit;


abstract public class AssertUnit {
  private String description;
  
  protected AssertUnit(String description) {
    this.description = description;
  }
  
  public String getDescription() { return this.description ; }
  
  abstract public boolean assertEvent(AssertEvent event);
}