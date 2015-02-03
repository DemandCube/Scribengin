package com.neverwinterdp.vm.command;

import com.neverwinterdp.vm.VM;

public class PingCommand extends Command {
  private String message = "Hello";

  public PingCommand() { super("ping") ; }
  
  public PingCommand(String message) { 
    this.message = message;
  }
  
  public String getMessage() { return message; }
  public void setMessage(String message) { this.message = message; }

  @Override
  public CommandResult<?> execute(VM vm) {
    CommandResult<String> result = new CommandResult<String>() ;
    result.setResult("Got your message: " + message);
    return result ;
  }
}
