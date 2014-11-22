package com.neverwinterdp.vm.command;

abstract public class Command {
  private String name;
  
  abstract public CommandResult execute() ;
}
