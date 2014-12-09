package com.neverwinterdp.scribengin.vm;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.neverwinterdp.scribengin.dataflow.DataflowDescriptor;
import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;

public class VMScribenginCommand {
  static public class DataflowDeployCommand extends Command {
    @JsonProperty
    private DataflowDescriptor descriptor ;
    
    public DataflowDeployCommand() { }
    
    public DataflowDeployCommand(DataflowDescriptor dataflowDescriptor) {
      this.descriptor = dataflowDescriptor;
    }
    
    @Override
    public CommandResult<Boolean> execute(VM vm) {
      System.out.println("Call VMScribenginCommand");
      CommandResult<Boolean> result = new CommandResult<Boolean>();
      VMScribenginMasterApp app = (VMScribenginMasterApp) vm.getVMApplication();
      try {
        app.getScribenginMaster().deploy(descriptor);
        result.setResult(true);
      } catch (Exception e) {
        result.setResult(false);
        result.setErrorStacktrace(ExceptionUtil.getStackTrace(e));
      }
      return result;
    }
    
  }
}
