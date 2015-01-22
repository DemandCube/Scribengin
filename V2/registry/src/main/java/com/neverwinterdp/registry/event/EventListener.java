package com.neverwinterdp.registry.event;


abstract public class EventListener {
  private String description;
  
  protected EventListener(String description) {
    this.description = description;
  }
  
  public String getDescription() { return this.description ; }
  
  abstract public boolean process(Event event);
}