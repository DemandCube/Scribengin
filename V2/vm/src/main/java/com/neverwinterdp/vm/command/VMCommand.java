package com.neverwinterdp.vm.command;

import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMApp.TerminateEvent;

public class VMCommand {
  static public class Terminate extends Command {
    private TerminateEvent event = TerminateEvent.Shutdown;
    
    public Terminate(String name, TerminateEvent event) { 
      super(name) ; 
      this.event = event;
    }
    
    public TerminateEvent getEvent() { return event; }

    public void setEvent(TerminateEvent event) { this.event = event; }

    @Override
    public CommandResult<?> execute(VM vm) {
      CommandResult<Boolean> result = new CommandResult<Boolean>() ;
      try {
        System.err.println("Terminate command with event " + event);
        vm.terminate(event);
        result.setResult(true);
      } catch (Exception e) {
        result.setResult(false);
        result.setErrorStacktrace(ExceptionUtil.getStackTrace(e));
      }
      return result ;
    }
  }
  
  static public class Shutdown extends Terminate {
    public Shutdown() { 
      super("shutdown", TerminateEvent.Shutdown) ; 
    }
  }
  
  static public class SimulateKill extends Terminate {
    public SimulateKill() { 
      super("simulate-kill", TerminateEvent.SimulateKill) ; 
    }
  }
  
  static public class Kill extends Terminate {
    public Kill() { 
      super("kill", TerminateEvent.Kill) ; 
    }
  }
}