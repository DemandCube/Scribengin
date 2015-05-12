package com.neverwinterdp.vm.command;

import com.neverwinterdp.registry.RegistryLogger;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMApp.TerminateEvent;

public class VMCommand {
  static public class Terminate extends Command {
    private TerminateEvent event = TerminateEvent.Shutdown;
    private long delay = 1000;
    
    public Terminate(String name, TerminateEvent event, long delay) { 
      super(name) ; 
      this.event = event;
      this.delay = delay ;
    }
    
    public TerminateEvent getEvent() { return event; }
    public void setEvent(TerminateEvent event) { this.event = event; }

    public long getDelay() { return delay; }
    public void setDelay(long delay) { this.delay = delay; }

    @Override
    public CommandResult<?> execute(VM vm) {
      CommandResult<Boolean> result = new CommandResult<Boolean>() ;
      try {
        vm.terminate(event, delay);
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
      super("shutdown", TerminateEvent.Shutdown, 1000) ; 
    }
  }
  
  static public class SimulateKill extends Terminate {
    public SimulateKill() { 
      super("simulate-kill", TerminateEvent.SimulateKill, 1000) ; 
    }
  }
  
  static public class Kill extends Terminate {
    public Kill() {
      super("kill", TerminateEvent.Kill, 3000) ; 
    }
    
    @Override
    public CommandResult<?> execute(VM vm) {
      try {
        RegistryLogger logger = new RegistryLogger(vm.getVMRegistry().getRegistry(), "/logger/kill") ;
        logger.info("kill", "call System.exit(0) to kill the vm " + vm.getDescriptor().getId());
      } catch(Exception ex) {
        ex.printStackTrace();
      }
      return super.execute(vm) ;
    }
  }
}