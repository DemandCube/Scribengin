package com.neverwinterdp.vm.command;

public class Payload {
  private Command command ;
  private CommandResult result ;
  
  public Command getCommand() { return command; }
  public void setCommand(Command command) { this.command = command; }
  
  public CommandResult getResult() { return result; }
  public void setResult(CommandResult result) { this.result = result; }
}
