package com.neverwinterdp.vm.command;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.neverwinterdp.vm.VM;

public class AppCommand {
  static public class Start extends Command {
    @JsonProperty()
    private String   app ;
    @JsonProperty()
    private String[] args ;

    public Start() { super("start") ; }
    
    public Start(String app, String[] args) { 
      this.app = app;
      this.args = args;
    }
    
    @Override
    public CommandResult<?> execute(VM vm) {
      CommandResult<Boolean> result = new CommandResult<Boolean>() ;
      try {
        vm.appStart(app, args);
        result.setResult(true);
      } catch (Exception e) {
        result.setResult(false);
        e.printStackTrace();
      }
      return result ;
    }
  }
  
  static public class AppStopCommand extends Command {

    public AppStopCommand() { super("stop") ; }
    
    @Override
    public CommandResult<?> execute(VM vm) {
      CommandResult<Boolean> result = new CommandResult<Boolean>() ;
      try {
        vm.appStop();
        result.setResult(true);
      } catch (Exception e) {
        result.setResult(false);
        e.printStackTrace();
      }
      return result ;
    }
  }
}
