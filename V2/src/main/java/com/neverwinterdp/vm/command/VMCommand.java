package com.neverwinterdp.vm.command;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.neverwinterdp.vm.VM;

public class VMCommand {
  static public class AppStart extends Command {
    @JsonProperty()
    private String   app ;
    @JsonProperty()
    private Map<String, String> properties ;

    public AppStart() { super("AppStart") ; }
    
    public AppStart(String app, Map<String, String> properties) { 
      this.app = app;
      this.properties = properties;
    }
    
    @Override
    public CommandResult<?> execute(VM vm) {
      CommandResult<Boolean> result = new CommandResult<Boolean>() ;
      try {
        vm.appStart(app, properties);
        result.setResult(true);
      } catch (Exception e) {
        result.setResult(false);
        e.printStackTrace();
      }
      return result ;
    }
  }
  
  static public class AppStop extends Command {

    public AppStop() { super("AppStop") ; }
    
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
  
  static public class Exit extends Command {
    public Exit() { super("Exit") ; }
    
    @Override
    public CommandResult<?> execute(VM vm) {
      CommandResult<Boolean> result = new CommandResult<Boolean>() ;
      try {
        vm.exit();
        result.setResult(true);
      } catch (Exception e) {
        result.setResult(false);
        e.printStackTrace();
      }
      return result ;
    }
  }
}