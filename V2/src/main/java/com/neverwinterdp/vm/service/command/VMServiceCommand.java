package com.neverwinterdp.vm.service.command;

import com.neverwinterdp.util.ExceptionUtil;
import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.command.Command;
import com.neverwinterdp.vm.command.CommandResult;
import com.neverwinterdp.vm.service.VMServiceApp;
import com.neverwinterdp.vm.service.VMService;

public class VMServiceCommand {
  static public class Allocate extends Command {
    private VMConfig vmConfig ;
    
    public Allocate() { 
      super("allocate"); 
    }
    
    public Allocate(VMConfig vmConfig) {
      this.vmConfig = vmConfig;
    }
    
    public VMConfig getVmConfig() { return vmConfig; }
    public void setVmConfig(VMConfig vmConfig) { this.vmConfig = vmConfig; }

    @Override
    public CommandResult<VMDescriptor> execute(VM vm) {
      VMServiceApp app = (VMServiceApp) vm.getVMApplication();
      VMService vmService = app.getVMService();
      CommandResult<VMDescriptor> result = new CommandResult<VMDescriptor>();
      try {
        //TODO: REVIEW this yarn configuration settings
        vmConfig.addYarnProperty(app.getVM().getDescriptor().getVmConfig().getYarnConf());
        VMDescriptor descriptor = vmService.allocate(vmConfig) ;
        result.setResult(descriptor);
      } catch (Exception e) {
        result.setErrorStacktrace(ExceptionUtil.getStackTrace(e));
      }
      return result;
    }
  }
  
  static public class Kill extends Command {
    private VMDescriptor vmDescriptor;
    
    public Kill() { super("kill"); }
    
    public Kill(VMDescriptor descriptor) {
      this.vmDescriptor = descriptor;
    }
    
    public VMDescriptor getVmDescriptor() { return vmDescriptor; }

    public void setVmDescriptor(VMDescriptor vmDescriptor) { this.vmDescriptor = vmDescriptor; }

    @Override
    public CommandResult<?> execute(VM vm) {
      VMServiceApp app = (VMServiceApp) vm.getVMApplication();
      VMService vmService = app.getVMService();
      CommandResult<Boolean> result = new CommandResult<Boolean>();
      try {
        vmService.kill(vmDescriptor); ;
        result.setResult(true);
      } catch (Exception e) {
        result.setResult(false);
        result.setErrorStacktrace(ExceptionUtil.getStackTrace(e));
      }
      return result;
    }
  }
}
