package com.neverwinterdp.vm.command;

import com.neverwinterdp.vm.VM;

abstract public class Command {
  private String name;
  
  public Command() {}
  
  public Command(String name) {
    this.name = name ;
  }
  
  public String getName() { return name; }
  public void setName(String name) { this.name = name; }

  abstract public CommandResult execute(VM vm) ;
}