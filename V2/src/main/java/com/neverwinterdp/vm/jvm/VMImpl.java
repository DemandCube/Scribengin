package com.neverwinterdp.vm.jvm;

import com.neverwinterdp.vm.VM;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.app.VMApplicationLogic;

public class VMImpl implements VM {
  private VMDescriptor descriptor ;
  private VMApplicationRunner vmApplicationRunner ;
  
  public VMImpl(VMDescriptor descriptor) {
    this.descriptor = descriptor;
  }
  
  public VMImpl(long id, int cpuCores, int memory) {
    descriptor = new VMDescriptor() ;
    descriptor.setId(id);
    descriptor.setCpuCores(cpuCores);
    descriptor.setMemory(memory);
  }
  
  @Override
  public VMDescriptor getDescriptor() { return descriptor; }

  @Override
  public void startApp(String vmAppClass, String[] args) throws Exception {
    if(vmApplicationRunner != null) {
      throw new Exception("VM Application is already started");
    }
    Class<VMApplicationLogic> vmAppType = (Class<VMApplicationLogic>)Class.forName(vmAppClass) ;
    VMApplicationLogic vmApp = vmAppType.newInstance();
    vmApplicationRunner = new VMApplicationRunner(vmApp, args) ;
    vmApplicationRunner.start();
  }
  
  @Override
  public void stopApp() throws Exception {
    if(vmApplicationRunner == null) return;
    vmApplicationRunner.interrupt();
  }
  
  @Override
  public void exit() throws Exception {
    stopApp();
  }
  
  static public class VMApplicationRunner extends Thread {
    private VMApplicationLogic vmApplication;
    private String[]  args ;
    
    public VMApplicationRunner(VMApplicationLogic vmApplication, String[] args) {
      this.vmApplication = vmApplication;
      this.args = args;
    }
    
    public void run() {
      try {
        vmApplication.run(args);
      } catch (InterruptedException e) {
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
