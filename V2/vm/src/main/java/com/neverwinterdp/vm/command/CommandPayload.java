package com.neverwinterdp.vm.command;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.neverwinterdp.util.JSONSerializer;

public class CommandPayload {
  private Command command ;
  private CommandResult<?> result ;
  
  public CommandPayload() {}
  
  public CommandPayload(Command command, CommandResult<?> result) {
    this.command = command;
    this.result = result ;
  }
  
  public <T extends Command> T getCommandAs(Class<T> type) { return (T) command; }
  
  @JsonDeserialize(using=JSONSerializer.GenericTypeDeserializer.class)
  public Command getCommand() { return command; }
  
  @JsonSerialize(using=JSONSerializer.GenericTypeSerializer.class)
  public void setCommand(Command command) { this.command = command; }
  
  public CommandResult<?> getResult() { return result; }
  public void setResult(CommandResult<?> result) { this.result = result; }
}
