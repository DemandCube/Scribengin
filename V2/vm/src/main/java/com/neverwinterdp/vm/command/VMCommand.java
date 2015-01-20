package com.neverwinterdp.vm.command;

import com.neverwinterdp.vm.VM;

public class VMCommand {
  static public class Shutdown extends Command {
    public Shutdown() { super("shutdown") ; }
    
    @Override
    public CommandResult<?> execute(VM vm) {
      CommandResult<Boolean> result = new CommandResult<Boolean>() ;
      try {
        vm.shutdown();
        result.setResult(true);
      } catch (Exception e) {
        result.setResult(false);
        e.printStackTrace();
      }
      return result ;
    }
  }
}